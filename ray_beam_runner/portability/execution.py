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
import logging
import random
import typing
from typing import Iterator, Tuple, Any, List
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import apache_beam
import ray
from apache_beam import coders

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


@ray.remote
def ray_execute_bundle(
    runner_context: 'RayRunnerExecutionContext',
    transform_buffer_coder: Mapping[str, typing.Tuple[bytes, str]],
    fired_timers: Mapping[translations.TimerFamilyId, fn_execution.PartitionableBuffer],
    expected_outputs: translations.DataOutput,
    expected_output_timers: Mapping[translations.TimerFamilyId, bytes],
    instruction_request_repr: Mapping[str, typing.Any],
    dry_run=False,
) -> Tuple[Any, List[Any]]:

  instruction_request = beam_fn_api_pb2.InstructionRequest(
    instruction_id=instruction_request_repr['instruction_id'],
    process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
      process_bundle_descriptor_id=instruction_request_repr['process_descriptor_id'],
      cache_tokens=[instruction_request_repr['cache_token']]))
  output_buffers = collections.defaultdict(list)
  process_bundle_id = instruction_request.instruction_id

  worker_handler: worker_handlers.WorkerHandler = worker_handlers.EmbeddedWorkerHandler(
      None, # Unnecessary payload.
      runner_context.state_servicer,
      None, # Unnecessary provision info.
      runner_context.worker_manager,
  )
  worker_handler.worker.bundle_processor_cache.register(
    runner_context.worker_manager.process_bundle_descriptor(instruction_request_repr['process_descriptor_id'])
  )

  for transform_id, timer_family_id in expected_output_timers.keys():
    timer_out = worker_handler.data_conn.output_timer_stream(
        process_bundle_id, transform_id, timer_family_id)
    for timer in fired_timers.get((transform_id, timer_family_id), []):
      timer_out.write(timer)
    timer_out.close()

  def fetch_data(transform_name, buffer_id, coder_id):
    if buffer_id.startswith(b'group'):
      _, pcoll_id = translations.split_buffer_id(buffer_id)
      transform = runner_context.pipeline_components.transforms[pcoll_id]
      out_pcoll = runner_context.pipeline_components.pcollections[transform.outputs['None']]
      windowing_strategy = runner_context.pipeline_components.windowing_strategies[out_pcoll.windowing_strategy_id]
      postcoder = runner_context.pipeline_context.coders[coder_id]
      precoder = coders.WindowedValueCoder(
        coders.TupleCoder((
          postcoder.wrapped_value_coder._coders[0],
          postcoder.wrapped_value_coder._coders[1]._elem_coder
        )),
        postcoder.window_coder)
      buffer = fn_execution.GroupingBuffer(
        pre_grouped_coder=precoder,
        post_grouped_coder=postcoder,
        windowing=apache_beam.Windowing.from_runner_api(windowing_strategy, None),
      )
    else:
      if isinstance(buffer_id, bytes) and (
          buffer_id.startswith(b'materialize') or buffer_id.startswith(b'timer')):
        buffer_id = buffer_id
      else:
        buffer_id = transform_name

      buffer = fn_execution.ListBuffer(coder_impl=runner_context.pipeline_context.coders[coder_id].get_impl())

    buffer_actor = ray.get(runner_context.pcollection_buffers.get.remote(buffer_id))
    for elm in ray.get(buffer_actor.fetch.remote()):
      buffer.append(elm)
    return buffer

  inputs = {
    k: fetch_data(k, buffer_id, coder_id)
    for k, (buffer_id, coder_id) in transform_buffer_coder.items()
  }

  for transform_id, elements in inputs.items():
    data_out = worker_handler.data_conn.output_stream(
        process_bundle_id, transform_id)
    for byte_stream in elements:
      data_out.write(byte_stream)
    data_out.close()

  expect_reads: List[typing.Union[str, translations.TimerFamilyId]] = list(expected_outputs.keys())
  expect_reads.extend(list(expected_output_timers.keys()))

  result_future = worker_handler.control_conn.push(instruction_request)

  for output in worker_handler.data_conn.input_elements(
      process_bundle_id,
      expect_reads,
      abort_callback=lambda:
      (result_future.is_done() and bool(result_future.get().error))):
    if isinstance(output, beam_fn_api_pb2.Elements.Timers) and not dry_run:
      output_buffers[expected_outputs[(output.transform_id, output.timer_family_id)]].append(output.data)
    if isinstance(output, beam_fn_api_pb2.Elements.Data) and not dry_run:
      output_buffers[expected_outputs[output.transform_id]].append(output.data)

  actor_calls = []
  for pcoll, buffer in output_buffers.items():
    buffer_actor = ray.get(runner_context.pcollection_buffers.get.remote(pcoll))
    actor_calls.append(buffer_actor.extend.remote(buffer))
  ray.get(actor_calls)

  return result_future.get().SerializeToString(), list(output_buffers.keys())


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
    self._process_bundle_descriptors[process_bundle_descriptor.id] = process_bundle_descriptor.SerializeToString()

  def process_bundle_descriptor(self, id):
    pbd = self._process_bundle_descriptors[id]
    if isinstance(pbd, beam_fn_api_pb2.ProcessBundleDescriptor):
      return pbd
    else:
      return beam_fn_api_pb2.ProcessBundleDescriptor.FromString(pbd)


class RayStage(translations.Stage):
  def __reduce__(self):
    data = (
      self.name,
      [t.SerializeToString() for t in self.transforms],
      self.downstream_side_inputs,
      [], #  self.must_follow,
      self.parent,
      self.environment,
      self.forced_root)
    deserializer = lambda *args: RayStage(
      args[0],
      [beam_runner_api_pb2.PTransform.FromString(s) for s in args[1]],
      args[2],
      args[3],
      args[4],
      args[5],
      args[6],)
    return (deserializer, data)

  @staticmethod
  def from_Stage(stage: translations.Stage):
    return RayStage(
      stage.name,
      stage.transforms,
      stage.downstream_side_inputs,
      # stage.must_follow,
      [],
      stage.parent,
      stage.environment,
      stage.forced_root,
    )


@ray.remote
class PcollectionActor:
  def __init__(self):
    self.buffer = []

  def append(self, data):
    self.buffer.append(data)

  def extend(self, buffer):
    for elm in buffer:
      self.buffer.append(elm)

  def fetch(self):
    return self.buffer

@ray.remote
class PcollectionBufferManager:
  def __init__(self):
    self.buffers = {}

  def register(self, pcoll):
    self.buffers[pcoll] = PcollectionActor.remote()

  def get(self, pcoll):
    if pcoll not in self.buffers:
      self.register(pcoll)
    return self.buffers[pcoll]


@ray.remote
class RayWatermarkManager(watermark_manager.WatermarkManager):
  def __init__(self):
    # the original WatermarkManager performs a lot of computation
    # in its __init__ method. Because Ray calls __init__ whenever
    # it deserializes an object, we'll move its setup elsewhere.
    self._initialized = False

  def setup(self, stages):
    if self._initialized:
      return
    logging.info('initialized the RayWatermarkManager')
    watermark_manager.WatermarkManager.__init__(self, stages)
    self._initialized = True


class RayRunnerExecutionContext(object):
  def __init__(self,
      stages: List[translations.Stage],
      pipeline_components: beam_runner_api_pb2.Components,
      safe_coders: translations.SafeCoderMapping,
      data_channel_coders: Mapping[str, str],
      state_servicer: Optional[RayStateManager] = None,
      worker_manager: Optional[RayWorkerHandlerManager] = None,
      pcollection_buffers: PcollectionBufferManager = None,
  ) -> None:
    ray.util.register_serializer(
      beam_runner_api_pb2.Components,
      serializer=lambda x: x.SerializeToString(),
      deserializer=lambda s: beam_runner_api_pb2.Components.FromString(s))
    ray.util.register_serializer(
      pipeline_context.PipelineContext,
      serializer=lambda x: x.proto.SerializeToString(),
      deserializer=lambda s: pipeline_context.PipelineContext(
        proto=beam_runner_api_pb2.Components.FromString(s)))

    self.pcollection_buffers = (
        pcollection_buffers or PcollectionBufferManager.remote())
    self.state_servicer = state_servicer or RayStateManager()
    self.stages = [RayStage.from_Stage(s)
                   if not isinstance(s, RayStage) else s for s in stages]
    self.side_input_descriptors_by_stage = (
        fn_execution.FnApiRunnerExecutionContext._build_data_side_inputs_map(
          stages))
    self.pipeline_components = pipeline_components
    self.safe_coders = safe_coders
    self.data_channel_coders = data_channel_coders

    self.input_transform_to_buffer_id = {
        t.unique_name: bytes(t.spec.payload)
        for s in stages for t in s.transforms
        if t.spec.urn == bundle_processor.DATA_INPUT_URN
    }
    self._watermark_manager = RayWatermarkManager.remote()
    self.pipeline_context = pipeline_context.PipelineContext(
        pipeline_components,
        iterable_state_write=False)
    self.safe_windowing_strategies = {
        # TODO: Enable safe_windowing_strategy after
        #  figuring out how to pickle the function.
        # id: self._make_safe_windowing_strategy(id)
        id: id
        for id in pipeline_components.windowing_strategies.keys()
    }
    self.stats = _RayRunnerStats.remote()
    self._uid = 0
    self.worker_manager = worker_manager or RayWorkerHandlerManager()
    self.timer_coder_ids = self._build_timer_coders_id_map()

  @property
  def watermark_manager(self):
    self._watermark_manager.setup.remote(self.stages)
    return self._watermark_manager

  def _build_timer_coders_id_map(self):
    # type: () -> Dict[Tuple[str, str], str]
    from apache_beam.utils import proto_utils
    timer_coder_ids = {}
    for transform_id, transform_proto in (self.pipeline_components.transforms.items()):
      if transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
        pardo_payload = proto_utils.parse_Bytes(
          transform_proto.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for id, timer_family_spec in pardo_payload.timer_family_specs.items():
          timer_coder_ids[(transform_id, id)] = (
            timer_family_spec.timer_family_coder_id)
    return timer_coder_ids

  @staticmethod
  def next_uid():
    # TODO(pabloem): Use stats actor for UIDs.
    # return str(ray.get(self.stats.next_bundle.remote()))
    # self._uid += 1
    return str(random.randint(0, 11111111))

  def __reduce__(self):
    # We need to implement custom serialization for this particular class
    # because it contains several members that are protocol buffers, and
    # protobufs are not pickleable due to being a C extension - so we serialize
    # protobufs to string before serialzing the rest of the object.
    data = (self.stages,
            self.pipeline_components.SerializeToString(),
            self.safe_coders,
            self.data_channel_coders,
            self.state_servicer,
            self.worker_manager,
            self.pcollection_buffers)
    deserializer = lambda *args: RayRunnerExecutionContext(
      args[0], beam_runner_api_pb2.Components.FromString(args[1]), args[2],
      args[3], args[4], args[5], args[6])
    return (deserializer, data)
