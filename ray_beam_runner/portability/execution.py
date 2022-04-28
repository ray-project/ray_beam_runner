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

"""Set of utilities for execution of a pipeline by the RayRunner."""

# mypy: disallow-untyped-defs

import contextlib
import collections
import copy
import logging
from typing import Iterator
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Tuple

import ray

from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability.fn_api_runner import execution as fn_execution
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner import watermark_manager
from apache_beam.runners.portability.fn_api_runner import worker_handlers
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import sdk_worker

_LOGGER = logging.getLogger(__name__)


def ray_execute_bundle(
    runner_context: 'RayRunnerExecutionContext',
    bundle_context_manager: fn_execution.BundleContextManager,
    state_handler: sdk_worker.StateHandler,
    inputs: Mapping[str, fn_execution.PartitionableBuffer],
    fired_timers: Mapping[translations.TimerFamilyId, fn_execution.PartitionableBuffer],
    expected_outputs: translations.DataOutput,
    expected_output_timers: Mapping[translations.TimerFamilyId, bytes],
    instruction_request: beam_fn_api_pb2.InstructionRequest,
    dry_run=False,
) -> Tuple[beam_fn_api_pb2.InstructionResponse, Mapping[bytes, fn_execution.PartitionableBuffer]]:
  output_buffers = {}
  process_bundle_id = instruction_request.instruction_id

  worker_handler: worker_handlers.WorkerHandler = worker_handlers.EmbeddedWorkerHandler(
      None, # Unnecessary payload.
      state_handler,
      None, # Unnecessary provision info.
      runner_context.worker_manager,
  )

  for transform_id, timer_family_id in expected_output_timers.keys():
    timer_out = worker_handler.data_conn.output_timer_stream(
        process_bundle_id, transform_id, timer_family_id)
    for timer in fired_timers.get((transform_id, timer_family_id), []):
      timer_out.write(timer)
    timer_out.close()

  for transform_id, elements in inputs.items():
    data_out = worker_handler.data_conn.output_stream(
        process_bundle_id, transform_id)
    for byte_stream in elements:
      data_out.write(byte_stream)
    data_out.close()

  expect_reads = list(
      expected_outputs.keys())  # type: List[Union[str, Tuple[str, str]]]
  expect_reads.extend(list(expected_output_timers.keys()))

  result_future = worker_handler.control_conn.push(instruction_request)

  # Gather all output data.
  for output in worker_handler.data_conn.input_elements(
      process_bundle_id,
      expect_reads,
      abort_callback=lambda:
      (result_future.is_done() and bool(result_future.get().error))):
    if isinstance(output, beam_fn_api_pb2.Elements.Timers) and not dry_run:
      timer_buffer = _get_buffer(
          runner_context,
          bundle_context_manager,
          expected_output_timers[(
              output.transform_id, output.timer_family_id)],
          output.transform_id,
          output_buffers)
      if timer_buffer.cleared:
        timer_buffer.reset()
      timer_buffer.append(output.timers)
    if isinstance(output, beam_fn_api_pb2.Elements.Data) and not dry_run:
      _get_buffer(
          runner_context,
          bundle_context_manager,
          expected_outputs[output.transform_id],
          output.transform_id,
          output_buffers).append(output.data)

  _LOGGER.debug('Wait for the bundle %s to finish.' % process_bundle_id)
  return result_future.get(), output_buffers


def _get_buffer(
    runner_context: 'RayRunnerExecutionContext',
    bundle_context_manager: fn_execution.BundleContextManager,
    buffer_id: bytes,
    transform_id: str,
    output_buffers: MutableMapping[bytes, fn_execution.PartitionableBuffer]):
  kind, name = translations.split_buffer_id(buffer_id)
  if kind == 'materialize':
    if buffer_id not in output_buffers:
      coder_id = beam_fn_api_pb2.RemoteGrpcPort.FromString(
          bundle_context_manager.process_bundle_descriptor.transforms[transform_id].spec.payload
      ).coder_id
      input_coder_impl = _get_coder_impl(coder_id, runner_context)
      output_buffers[buffer_id] = fn_execution.ListBuffer(input_coder_impl)
    return output_buffers[buffer_id]
  elif kind == 'timers':
    # For timer buffer, name = timer_family_id
    if buffer_id not in output_buffers:
      timer_coder_impl = _get_coder_impl(bundle_context_manager._timer_coder_ids[(transform_id, name)], runner_context)
      output_buffers[buffer_id] = fn_execution.ListBuffer(timer_coder_impl)
    return output_buffers[buffer_id]
  elif kind == 'group':
    if buffer_id not in output_buffers:
      original_gbk_transform = name
      transform_proto = runner_context.pipeline_components.transforms[
        original_gbk_transform]
      input_pcoll = translations.only_element(list(transform_proto.inputs.values()))
      output_pcoll = translations.only_element(list(transform_proto.outputs.values()))
      pre_gbk_coder = runner_context.pipeline_context.coders[
        runner_context.safe_coders[
          runner_context.data_channel_coders[input_pcoll]]]
      post_gbk_coder = runner_context.pipeline_context.coders[
        runner_context.safe_coders[
          runner_context.data_channel_coders[output_pcoll]]]
      windowing_strategy = (
          runner_context.pipeline_context.windowing_strategies[
            runner_context.safe_windowing_strategies[
              runner_context.pipeline_components.
                pcollections[input_pcoll].windowing_strategy_id]])
      output_buffers[buffer_id] = fn_execution.GroupingBuffer(
          pre_gbk_coder, post_gbk_coder, windowing_strategy)
    return output_buffers[buffer_id]
  else:
    raise ValueError('Unknown kind: %s' % kind)

def _get_coder_impl(coder_id, execution_context: 'RayRunnerExecutionContext'):
  if coder_id in execution_context.safe_coders:
    return execution_context.pipeline_context.coders[
      execution_context.safe_coders[coder_id]].get_impl()
  else:
    return execution_context.pipeline_context.coders[coder_id].get_impl()


class _RayMetricsActor:
  def __init__(self):
    self._metrics = {}


@ray.remote
class _RayRunnerStats:
  def __init__(self):
    self._bundle_uid = 0

  def next_bundle(self):
    self._bundle_uid += 1
    return self._bundle_uid


@ray.remote
class _ActorStateManager:
  def __init__(self):
    self._data = collections.defaultdict(lambda : [])

  def get_raw(
      self,
      bundle_id: str,
      state_key: str,
      continuation_token: Optional[bytes] = None,
  ) -> Tuple[bytes, Optional[bytes]]:
    if continuation_token:
      continuation_token = int(continuation_token)
    else:
      continuation_token = 0

    new_cont_token = continuation_token + 1
    if len(self._data[(bundle_id, state_key)]) == new_cont_token:
      return self._data[(bundle_id, state_key)][continuation_token], None
    else:
      return (self._data[(bundle_id, state_key)][continuation_token],
              str(continuation_token + 1).encode('utf8'))

  def append_raw(
      self,
      bundle_id: str,
      state_key: str,
      data: bytes
  ):
    self._data[(bundle_id, state_key)].append(data)

  def clear(self, bundle_id: str, state_key: str):
    self._data[(bundle_id, state_key)] = []


class RayStateManager(sdk_worker.StateHandler):
  def __init__(self, state_actor: Optional[_ActorStateManager] = None):
    self._state_actor = state_actor or _ActorStateManager.remote()
    self._instruction_id: Optional[str] = None

  @staticmethod
  def _to_key(state_key: beam_fn_api_pb2.StateKey):
    return state_key.SerializeToString()

  def get_raw(
      self,
      state_key,  # type: beam_fn_api_pb2.StateKey
      continuation_token=None  # type: Optional[bytes]
  ) -> Tuple[bytes, Optional[bytes]]:
    assert self._instruction_id is not None
    return ray.get(
        self._state_actor.get_raw.remote(self._instruction_id, RayStateManager._to_key(state_key), continuation_token))

  def append_raw(
      self,
      state_key: beam_fn_api_pb2.StateKey,
      data: bytes
  ) -> sdk_worker._Future:
    assert self._instruction_id is not None
    return self._state_actor.append_raw.remote(self._instruction_id, RayStateManager._to_key(state_key), data)

  def clear(self, state_key: beam_fn_api_pb2.StateKey) -> sdk_worker._Future:
    # TODO(pabloem): Does the ray future work as a replacement of Beam _Future?
    assert self._instruction_id is not None
    return self._state_actor.clear.remote(self._instruction_id, RayStateManager._to_key(state_key))

  @contextlib.contextmanager
  def process_instruction_id(self, bundle_id: str) -> Iterator[None]:
    self._instruction_id = bundle_id
    yield
    self._instruction_id = None

  def done(self):
    pass


class RayWorkerHandlerManager:
  def __init__(self):
    self._process_bundle_descriptors = {}

  def register_process_bundle_descriptor(self, process_bundle_descriptor: beam_fn_api_pb2.ProcessBundleDescriptor):
    self._process_bundle_descriptors[process_bundle_descriptor.id] = process_bundle_descriptor


class RayRunnerExecutionContext(object):
  def __init__(self,
      stages: List[translations.Stage],
      pipeline_components: beam_runner_api_pb2.Components,
      safe_coders: translations.SafeCoderMapping,
      data_channel_coders: Mapping[str, str],
  ) -> None:
    """
    :param pipeline_components:  (beam_runner_api_pb2.Components)
    :param safe_coders: A map from Coder ID to Safe Coder ID.
    :param data_channel_coders: A map from PCollection ID to the ID of the Coder
        for that PCollection.
    """
    self.state_servicer = RayStateManager()
    self.stages = stages
    self.side_input_descriptors_by_stage = (
        fn_execution.FnApiRunnerExecutionContext._build_data_side_inputs_map(stages))
    self.pipeline_components = pipeline_components
    self.safe_coders = safe_coders
    self.data_channel_coders = data_channel_coders

    self.input_transform_to_buffer_id = {
        t.unique_name: t.spec.payload
        for s in stages for t in s.transforms
        if t.spec.urn == bundle_processor.DATA_INPUT_URN
    }
    self.watermark_manager = watermark_manager.WatermarkManager(stages)
    self.pipeline_context = pipeline_context.PipelineContext(
        self.pipeline_components,
        iterable_state_write=False)
    self.safe_windowing_strategies = {
        id: self._make_safe_windowing_strategy(id)
        for id in self.pipeline_components.windowing_strategies.keys()
    }
    self.stats = _RayRunnerStats.remote()
    self.worker_manager = RayWorkerHandlerManager()

  def next_uid(self) -> str:
    return str(ray.get(self.stats.next_bundle.remote()))

  def _make_safe_windowing_strategy(self, id):
    # type: (str) -> str
    windowing_strategy_proto = self.pipeline_components.windowing_strategies[id]
    if windowing_strategy_proto.window_fn.urn in fn_execution.SAFE_WINDOW_FNS:
      return id
    else:
      safe_id = id + '_safe'
      while safe_id in self.pipeline_components.windowing_strategies:
        safe_id += '_'
      safe_proto = copy.copy(windowing_strategy_proto)
      if (windowing_strategy_proto.merge_status ==
          beam_runner_api_pb2.MergeStatus.NON_MERGING):
        safe_proto.window_fn.urn = fn_execution.GenericNonMergingWindowFn.URN
        safe_proto.window_fn.payload = (
            windowing_strategy_proto.window_coder_id.encode('utf-8'))
      elif (windowing_strategy_proto.merge_status ==
            beam_runner_api_pb2.MergeStatus.NEEDS_MERGE):
        window_fn = fn_execution.GenericMergingWindowFn(self, windowing_strategy_proto)
        safe_proto.window_fn.urn = fn_execution.GenericMergingWindowFn.URN
        safe_proto.window_fn.payload = window_fn.payload()
      else:
        raise NotImplementedError(
            'Unsupported merging strategy: %s' %
            windowing_strategy_proto.merge_status)
      self.pipeline_context.windowing_strategies.put_proto(safe_id, safe_proto)
      return safe_id

  def commit_side_inputs_to_state(
      self,
      data_side_input,  # type: DataSideInput
  ):
    # type: (...) -> None
    for (consuming_transform_id, tag), (buffer_id,
                                        func_spec) in data_side_input.items():
      _, pcoll_id = translations.split_buffer_id(buffer_id)
      value_coder = self.pipeline_context.coders[self.safe_coders[
        self.data_channel_coders[pcoll_id]]]
      elements_by_window = fn_execution.WindowGroupingBuffer(func_spec, value_coder)
      if buffer_id not in self.pcoll_buffers:
        self.pcoll_buffers[buffer_id] = fn_execution.ListBuffer(
            coder_impl=value_coder.get_impl())
      for element_data in self.pcoll_buffers[buffer_id]:
        elements_by_window.append(element_data)

      if func_spec.urn == common_urns.side_inputs.ITERABLE.urn:
        for _, window, elements_data in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              iterable_side_input=beam_fn_api_pb2.StateKey.IterableSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window))
          self.state_servicer.append_raw(state_key, elements_data)
      elif func_spec.urn == common_urns.side_inputs.MULTIMAP.urn:
        for key, window, elements_data in elements_by_window.encoded_items():
          state_key = beam_fn_api_pb2.StateKey(
              multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                  transform_id=consuming_transform_id,
                  side_input_id=tag,
                  window=window,
                  key=key))
          self.state_servicer.append_raw(state_key, elements_data)
      else:
        raise ValueError("Unknown access pattern: '%s'" % func_spec.urn)