#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Set of utilities that perform static analysis for a Beam graph ahead of execution."""
import logging
import typing

from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability.fn_api_runner import translations as beam_translations
from apache_beam.runners.portability.fn_api_runner.translations import Stage
from apache_beam.runners.portability.fn_api_runner.translations import TransformContext
from apache_beam.runners.worker import bundle_processor


class StageTags:
  GROUPING_SHUFFLE = 'GroupingShuffle'
  RANDOM_SHUFFLE = 'RandomShuffle'


class _RayRunnerStage(Stage):
  def __init__(self, name, transforms, *args, tags: typing.Set[str] = None, **kwargs):
    super().__init__(name, transforms, *args, **kwargs)
    self.tags: typing.Set[str] = tags or set()


def expand_reshuffle(stages: typing.Iterable[Stage], pipeline_context: TransformContext) -> typing.Iterator[Stage]:
  for s in stages:
    t = beam_translations.only_transform(s.transforms)
    if t.spec.urn == common_urns.composites.RESHUFFLE.urn:
      reshuffle_buffer = beam_translations.create_buffer_id(s.name)
      reshuffle_write = _RayRunnerStage(
        t.unique_name + '/Write',
        [
          beam_runner_api_pb2.PTransform(
            unique_name=t.unique_name + '/Write',
            inputs=t.inputs,
            spec=beam_runner_api_pb2.FunctionSpec(
              urn=bundle_processor.DATA_OUTPUT_URN,
              payload=reshuffle_buffer))
        ],
        downstream_side_inputs=frozenset(),
        must_follow=s.must_follow,
        tags={StageTags.RANDOM_SHUFFLE}
      )
      yield reshuffle_write

      yield _RayRunnerStage(
        t.unique_name + '/Read',
        [
          beam_runner_api_pb2.PTransform(
            unique_name=t.unique_name + '/Read',
            outputs=t.outputs,
            spec=beam_runner_api_pb2.FunctionSpec(
              urn=bundle_processor.DATA_INPUT_URN,
              payload=reshuffle_buffer))
        ],
        downstream_side_inputs=s.downstream_side_inputs,
        must_follow=beam_translations.union(frozenset([reshuffle_write]), s.must_follow))
    else:
      yield s


def expand_gbk(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

  """Transforms each GBK into a write followed by a read."""
  for stage in stages:
    transform = beam_translations.only_transform(stage.transforms)
    if transform.spec.urn == common_urns.primitives.GROUP_BY_KEY.urn:
      for pcoll_id in transform.inputs.values():
        pipeline_context.length_prefix_pcoll_coders(pcoll_id)
      for pcoll_id in transform.outputs.values():
        if pipeline_context.use_state_iterables:
          pipeline_context.components.pcollections[
            pcoll_id].coder_id = pipeline_context.with_state_iterables(
            pipeline_context.components.pcollections[pcoll_id].coder_id)
        pipeline_context.length_prefix_pcoll_coders(pcoll_id)

      # This is used later to correlate the read and write.
      transform_id = stage.name
      if transform != pipeline_context.components.transforms.get(transform_id):
        transform_id = beam_translations.unique_name(
          pipeline_context.components.transforms, stage.name)
        pipeline_context.components.transforms[transform_id].CopyFrom(transform)
      gbk_buffer = beam_translations.create_buffer_id(transform_id)
      gbk_write = _RayRunnerStage(
        transform.unique_name + '/Write',
        [
          beam_runner_api_pb2.PTransform(
            unique_name=transform.unique_name + '/Write',
            inputs=transform.inputs,
            spec=beam_runner_api_pb2.FunctionSpec(
              urn=bundle_processor.DATA_OUTPUT_URN,
              payload=gbk_buffer))
        ],
        downstream_side_inputs=frozenset(),
        must_follow=stage.must_follow,
        tags={StageTags.GROUPING_SHUFFLE})
      yield gbk_write

      yield Stage(
        transform.unique_name + '/Read',
        [
          beam_runner_api_pb2.PTransform(
            unique_name=transform.unique_name + '/Read',
            outputs=transform.outputs,
            spec=beam_runner_api_pb2.FunctionSpec(
              urn=bundle_processor.DATA_INPUT_URN,
              payload=gbk_buffer))
        ],
        downstream_side_inputs=stage.downstream_side_inputs,
        must_follow=beam_translations.union(frozenset([gbk_write]), stage.must_follow))
    else:
      yield stage
