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

"""A PipelineRunner using the SDK harness."""
# pytype: skip-file
# mypy: check-untyped-defs
import collections
import copy
import logging
import typing
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union

from apache_beam.options import pipeline_options
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.pipeline import Pipeline
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import runner
from apache_beam.runners.common import group_by_key_input_visitor
from apache_beam.runners.portability.fn_api_runner import execution
from apache_beam.runners.portability.fn_api_runner import fn_runner
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner.execution import ListBuffer
from apache_beam.transforms import environments
from apache_beam.utils import timestamp
from apache_beam.utils import proto_utils

import ray
from ray_beam_runner.portability.context_management import RayBundleContextManager
from ray_beam_runner.portability.execution import Bundle
from ray_beam_runner.portability.execution import ray_execute_bundle, merge_stage_results
from ray_beam_runner.portability.execution import RayRunnerExecutionContext

_LOGGER = logging.getLogger(__name__)

# This module is experimental. No backwards-compatibility guarantees.


def _setup_options(options: pipeline_options.PipelineOptions):
  """Perform any necessary checkups and updates to input pipeline options"""

  # TODO(pabloem): Add input pipeline options
  RuntimeValueProvider.set_runtime_options({})

  experiments = (
      options.view_as(pipeline_options.DebugOptions).experiments or [])
  if not 'beam_fn_api' in experiments:
    experiments.append('beam_fn_api')
  options.view_as(pipeline_options.DebugOptions).experiments = experiments


def _check_supported_requirements(pipeline_proto: beam_runner_api_pb2.Pipeline, supported_requirements: typing.Iterable[str]):
  """Check that the input pipeline does not have unsuported requirements."""
  for requirement in pipeline_proto.requirements:
    if requirement not in supported_requirements:
      raise ValueError(
        'Unable to run pipeline with requirement: %s' % requirement)
  for transform in pipeline_proto.components.transforms.values():
    if transform.spec.urn == common_urns.primitives.TEST_STREAM.urn:
      raise NotImplementedError(transform.spec.urn)
    elif transform.spec.urn in translations.PAR_DO_URNS:
      payload = proto_utils.parse_Bytes(
        transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
      for timer in payload.timer_family_specs.values():
        if timer.time_domain != beam_runner_api_pb2.TimeDomain.EVENT_TIME:
          raise NotImplementedError(timer.time_domain)


def _pipeline_checks(pipeline: Pipeline, options: pipeline_options.PipelineOptions, supported_requirements: typing.Iterable[str]):
  # This is sometimes needed if type checking is disabled
  # to enforce that the inputs (and outputs) of GroupByKey operations
  # are known to be KVs.
  pipeline.visit(
    group_by_key_input_visitor(
      not options.view_as(pipeline_options.TypeOptions).
        allow_non_deterministic_key_coders))

  pipeline_proto = pipeline.to_runner_api(default_environment=environments.EmbeddedPythonEnvironment.default())
  fn_runner.FnApiRunner._validate_requirements(None, pipeline_proto)

  _check_supported_requirements(pipeline_proto, supported_requirements)
  return pipeline_proto


class RayFnApiRunner(runner.PipelineRunner):

  def __init__(
      self,
  ) -> None:

    """Creates a new Ray Runner instance.

    Args:
      progress_request_frequency: The frequency (in seconds) that the runner
          waits before requesting progress from the SDK.
    """
    super().__init__()
    # TODO: figure out if this is necessary (probably, later)
    self._progress_frequency = None
    self._cache_token_generator = fn_runner.FnApiRunner.get_cache_token_generator()

  @staticmethod
  def supported_requirements():
    # type: () -> Tuple[str, ...]
    return (
        common_urns.requirements.REQUIRES_STATEFUL_PROCESSING.urn,
        common_urns.requirements.REQUIRES_BUNDLE_FINALIZATION.urn,
        common_urns.requirements.REQUIRES_SPLITTABLE_DOFN.urn,
    )

  def run_pipeline(self,
                   pipeline: Pipeline,
                   options: pipeline_options.PipelineOptions
                  ) -> 'RayRunnerResult':

    # Checkup and set up input pipeline options
    _setup_options(options)

    # Check pipeline and convert into protocol buffer representation
    pipeline_proto = _pipeline_checks(pipeline, options, self.supported_requirements())

    # Take the protocol buffer representation of the user's pipeline, and
    # apply optimizations.
    stage_context, stages = translations.create_and_optimize_stages(
      copy.deepcopy(pipeline_proto),
      phases=[
        # This is a list of transformations and optimizations to apply
        # to a pipeline.
        translations.annotate_downstream_side_inputs,
        translations.fix_side_input_pcoll_coders,
        translations.pack_combiners,
        translations.lift_combiners,
        translations.expand_sdf,
        translations.expand_gbk,
        translations.sink_flattens,
        translations.greedily_fuse,
        translations.read_to_impulse,
        translations.impulse_to_input,
        translations.sort_stages,
        translations.setup_timer_mapping,
        translations.populate_data_channel_coders,
      ],
      known_runner_urns=frozenset([
        common_urns.primitives.FLATTEN.urn,
        common_urns.primitives.GROUP_BY_KEY.urn,
      ]),
      use_state_iterables=False,
      is_drain=False)
    return self.execute_pipeline(stage_context, stages)

  def execute_pipeline(self,
                       stage_context: translations.TransformContext,
                       stages: List[translations.Stage]
                       ) -> 'RayRunnerResult':
    """Execute pipeline represented by a list of stages and a context."""
    logging.info('Starting pipeline of %d stages.' % len(stages))

    runner_execution_context = RayRunnerExecutionContext(
        stages,
        stage_context.components,
        stage_context.safe_coders,
        stage_context.data_channel_coders)

    # Using this queue to hold 'bundles' that are ready to be processed
    queue = collections.deque()

    try:
      for stage in stages:
        bundle_ctx = RayBundleContextManager(runner_execution_context, stage)
        stage_results = self._run_stage(runner_execution_context, bundle_ctx, queue)
    finally:
      pass
    return RayRunnerResult(runner.PipelineState.DONE)

  def _run_stage(self,
                 runner_execution_context: RayRunnerExecutionContext,
                 bundle_context_manager: RayBundleContextManager,
                 ready_bundles: collections.deque,
                ) -> beam_fn_api_pb2.InstructionResponse:

    """Run an individual stage.

    Args:
      runner_execution_context: An object containing execution information for
        the pipeline.
      bundle_context_manager (execution.BundleContextManager): A description of
        the stage to execute, and its context.
    """
    bundle_context_manager.setup()
    runner_execution_context.worker_manager.register_process_bundle_descriptor(
        bundle_context_manager.process_bundle_descriptor)
    input_timers: Mapping[translations.TimerFamilyId,
                          execution.PartitionableBuffer] = {}

    _LOGGER.info('Running %s', bundle_context_manager.stage.name)

    final_result = None  # type: Optional[beam_fn_api_pb2.InstructionResponse]

    while True:
      last_result, deferred_inputs, fired_timers, watermark_updates, bundle_outputs = (
          self._run_bundle(
              runner_execution_context,
              bundle_context_manager,
              input_timers,))

      for pc_name, watermark in watermark_updates.items():
        runner_execution_context.watermark_manager.set_pcoll_watermark.remote(
            pc_name, watermark)

      final_result = merge_stage_results(final_result, last_result)
      if not deferred_inputs and not fired_timers:
        break
      else:
        assert (ray.get(runner_execution_context.watermark_manager.get_stage_node.remote(
            bundle_context_manager.stage.name)).output_watermark()
                < timestamp.MAX_TIMESTAMP), (
            'wrong timestamp for %s. '
            % ray.get(runner_execution_context.watermark_manager.get_stage_node.remote(
            bundle_context_manager.stage.name)))
        data_input = deferred_inputs
        input_timers = fired_timers

    # Store the required downstream side inputs into state so it is accessible
    # for the worker when it runs bundles that consume this stage's output.
    data_side_input = (
        runner_execution_context.side_input_descriptors_by_stage.get(
            bundle_context_manager.stage.name, {}))
    # TODO(pabloem): Make sure that side inputs are being stored somewhere.
    # runner_execution_context.commit_side_inputs_to_state(data_side_input)

    return final_result

  def _run_bundle(
      self,
      runner_execution_context: RayRunnerExecutionContext,
      bundle_context_manager: RayBundleContextManager,
      input_timers: Mapping[translations.TimerFamilyId, execution.PartitionableBuffer],
  ) -> Tuple[beam_fn_api_pb2.InstructionResponse,
             Dict[str, execution.PartitionableBuffer],
             Dict[translations.TimerFamilyId, ListBuffer],
             Dict[Union[str, translations.TimerFamilyId], timestamp.Timestamp],
             List[Union[str, translations.TimerFamilyId]]]:
    """Execute a bundle, and return a result object, and deferred inputs."""
    transform_to_buffer_coder, data_output, stage_timers = (
      bundle_context_manager.get_bundle_inputs_and_outputs())

    logging.info("Running bundle with input data from: %s" % list(transform_to_buffer_coder.keys()))
    cache_token_generator = fn_runner.FnApiRunner.get_cache_token_generator(static=False)

  # TODO(pabloem): Are there two different IDs? the Bundle ID and PBD ID?
    process_bundle_id = 'bundle_%s' % bundle_context_manager.process_bundle_descriptor.id

    (result_str, output) = ray.get(ray_execute_bundle.remote(
        runner_execution_context,
        Bundle(input_timers=input_timers, input_data={}),
        transform_to_buffer_coder,
        data_output,
      stage_timers,
        instruction_request_repr={
          'instruction_id': process_bundle_id,
          'process_descriptor_id': bundle_context_manager.process_bundle_descriptor.id,
          'cache_token': next(cache_token_generator)
        }
    ))
    result = beam_fn_api_pb2.InstructionResponse.FromString(result_str)

    # TODO(pabloem): Add support for splitting of results.

    # Now we collect all the deferred inputs remaining from bundle execution.
    # Deferred inputs can be:
    # - timers
    # - SDK-initiated deferred applications of root elements
    # - Runner-initiated deferred applications of root elements
    deferred_inputs: Dict[str, execution.PartitionableBuffer] = {}

    # After collecting deferred inputs, we 'pad' the structure with empty
    # buffers for other expected inputs.
    # if deferred_inputs or newly_set_timers:
    #   # The worker will be waiting on these inputs as well.
    #   for other_input in data_input:
    #     if other_input not in deferred_inputs:
    #       deferred_inputs[other_input] = ListBuffer(
    #           coder_impl=bundle_context_manager.get_input_coder_impl(
    #               other_input))

    newly_set_timers = {}
    watermark_updates = {}
    return result, deferred_inputs, newly_set_timers, watermark_updates, output


class RayRunnerResult(runner.PipelineResult):
  def __init__(self, state):
    super().__init__(state)

  def wait_until_finish(self, duration=None):
    return None

  def metrics(self):
    """Returns a queryable object including user metrics only."""
    # TODO(pabloem): Implement this based on _RayMetricsActor
    raise NotImplementedError()

  def monitoring_metrics(self):
    """Returns a queryable object including all metrics."""
    # TODO(pabloem): Implement this based on _RayMetricsActor
    raise NotImplementedError()
