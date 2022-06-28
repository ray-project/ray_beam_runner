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

import collections
import dataclasses
import itertools
import logging
import random
import typing
from typing import Any
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import ray

import apache_beam
from apache_beam import coders
from apache_beam.metrics import monitoring_infos
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability.fn_api_runner import execution as fn_execution
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner import watermark_manager
from apache_beam.runners.portability.fn_api_runner import worker_handlers
from apache_beam.runners.worker import bundle_processor

from ray_beam_runner.portability.state import RayStateManager

_LOGGER = logging.getLogger(__name__)


@ray.remote
def ray_execute_bundle(
    runner_context: "RayRunnerExecutionContext",
    input_bundle: "Bundle",
    transform_buffer_coder: Mapping[str, typing.Tuple[bytes, str]],
    expected_outputs: translations.DataOutput,
    stage_timers: Mapping[translations.TimerFamilyId, bytes],
    instruction_request_repr: Mapping[str, typing.Any],
    dry_run=False,
) -> Tuple[str, List[Any], Mapping[str, ray.ObjectRef]]:

    instruction_request = beam_fn_api_pb2.InstructionRequest(
        instruction_id=instruction_request_repr["instruction_id"],
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_id=instruction_request_repr[
                "process_descriptor_id"
            ],
            cache_tokens=[instruction_request_repr["cache_token"]],
        ),
    )
    output_buffers: Mapping[
        typing.Union[str, translations.TimerFamilyId], list
    ] = collections.defaultdict(list)
    process_bundle_id = instruction_request.instruction_id

    worker_handler = _get_worker_handler(
        runner_context, instruction_request_repr["process_descriptor_id"]
    )

    _send_timers(worker_handler, input_bundle, stage_timers, process_bundle_id)

    input_data = {
        k: _fetch_decode_data(
            runner_context,
            _get_input_id(transform_buffer_coder[k][0], k),
            transform_buffer_coder[k][1],
            objrefs,
        )
        for k, objrefs in input_bundle.input_data.items()
    }

    for transform_id, elements in input_data.items():
        data_out = worker_handler.data_conn.output_stream(
            process_bundle_id, transform_id
        )
        for byte_stream in elements:
            data_out.write(byte_stream)
        data_out.close()

    expect_reads: List[typing.Union[str, translations.TimerFamilyId]] = list(
        expected_outputs.keys()
    )
    expect_reads.extend(list(stage_timers.keys()))

    result_future = worker_handler.control_conn.push(instruction_request)

    for output in worker_handler.data_conn.input_elements(
        process_bundle_id,
        expect_reads,
        abort_callback=lambda: (
            result_future.is_done() and bool(result_future.get().error)
        ),
    ):
        if isinstance(output, beam_fn_api_pb2.Elements.Timers) and not dry_run:
            output_buffers[
                stage_timers[(output.transform_id, output.timer_family_id)]
            ].append(output.timers)
        if isinstance(output, beam_fn_api_pb2.Elements.Data) and not dry_run:
            output_buffers[expected_outputs[output.transform_id]].append(output.data)

    for pcoll, buffer in output_buffers.items():
        objrefs = [ray.put(buffer)]
        runner_context.pcollection_buffers.put.remote(pcoll, objrefs)
        output_buffers[pcoll] = objrefs

    result: beam_fn_api_pb2.InstructionResponse = result_future.get()

    # Now we collect all the deferred inputs remaining from bundle execution.
    # Deferred inputs can be:
    # - timers
    # - SDK-initiated deferred applications of root elements
    # - # TODO: Runner-initiated deferred applications of root elements
    delayed_applications = _retrieve_delayed_applications(
        result,
        runner_context.worker_manager.process_bundle_descriptor(
            instruction_request_repr["process_descriptor_id"]
        ),
        runner_context,
    )

    return result.SerializeToString(), list(output_buffers.keys()), delayed_applications


def _retrieve_delayed_applications(
    bundle_result: beam_fn_api_pb2.InstructionResponse,
    process_bundle_descriptor: beam_fn_api_pb2.ProcessBundleDescriptor,
    runner_context: "RayRunnerExecutionContext",
):
    """Extract delayed applications from a bundle run.

    A delayed application represents a user-initiated checkpoint, where user code
    delays the consumption of a data element to checkpoint the previous elements
    in a bundle.
    """
    delayed_bundles = {}
    for delayed_application in bundle_result.process_bundle.residual_roots:
        # TODO(pabloem): Time delay needed for streaming. For now we'll ignore it.
        # time_delay = delayed_application.requested_time_delay
        transform = process_bundle_descriptor.transforms[
            delayed_application.application.transform_id
        ]
        pcoll_name = transform.inputs[delayed_application.application.input_id]

        consumer_transform = translations.only_element(
            [
                read_id
                for read_id, proto in process_bundle_descriptor.transforms.items()
                if proto.spec.urn == bundle_processor.DATA_INPUT_URN
                and pcoll_name in proto.outputs.values()
            ]
        )
        if consumer_transform not in delayed_bundles:
            delayed_bundles[consumer_transform] = []
        delayed_bundles[consumer_transform].append(
            delayed_application.application.element
        )

    for consumer, data in delayed_bundles.items():
        ref = ray.put([data])
        runner_context.pcollection_buffers.put.remote(consumer, [ref])
        delayed_bundles[consumer] = ref

    return delayed_bundles


def _get_input_id(buffer_id, transform_name):
    """Get the 'buffer_id' for the input data we're retrieving.

    For most data, the buffer ID is as expected, but for IMPULSE readers, the
    buffer ID is the consumer name.
    """
    if isinstance(buffer_id, bytes) and (
        buffer_id.startswith(b"materialize")
        or buffer_id.startswith(b"timer")
        or buffer_id.startswith(b"group")
    ):
        buffer_id = buffer_id
    else:
        buffer_id = transform_name.encode("ascii")
    return buffer_id


def _fetch_decode_data(
    runner_context: "RayRunnerExecutionContext",
    buffer_id: bytes,
    coder_id: str,
    data_references: List[ray.ObjectRef],
):
    """Fetch a PCollection's data and decode it."""
    if buffer_id.startswith(b"group"):
        _, pcoll_id = translations.split_buffer_id(buffer_id)
        transform = runner_context.pipeline_components.transforms[pcoll_id]
        out_pcoll = runner_context.pipeline_components.pcollections[
            translations.only_element(transform.outputs.values())
        ]
        windowing_strategy = runner_context.pipeline_components.windowing_strategies[
            out_pcoll.windowing_strategy_id
        ]
        postcoder = runner_context.pipeline_context.coders[coder_id]
        precoder = coders.WindowedValueCoder(
            coders.TupleCoder(
                (
                    postcoder.wrapped_value_coder._coders[0],
                    postcoder.wrapped_value_coder._coders[1]._elem_coder,
                )
            ),
            postcoder.window_coder,
        )
        buffer = fn_execution.GroupingBuffer(
            pre_grouped_coder=precoder,
            post_grouped_coder=postcoder,
            windowing=apache_beam.Windowing.from_runner_api(windowing_strategy, None),
        )
    else:
        buffer = fn_execution.ListBuffer(
            coder_impl=runner_context.pipeline_context.coders[coder_id].get_impl()
        )

    for block in ray.get(data_references):
        # TODO(pabloem): Stop using ListBuffer, and use different
        #  buffers to pass data to Beam.
        for elm in block:
            buffer.append(elm)
    return buffer


def _send_timers(
    worker_handler: worker_handlers.WorkerHandler,
    input_bundle: "Bundle",
    stage_timers: Mapping[translations.TimerFamilyId, bytes],
    process_bundle_id,
) -> None:
    """Pass timers to the worker for processing."""
    for transform_id, timer_family_id in stage_timers.keys():
        timer_out = worker_handler.data_conn.output_timer_stream(
            process_bundle_id, transform_id, timer_family_id
        )
        for timer in input_bundle.input_timers.get((transform_id, timer_family_id), []):
            timer_out.write(timer)
        timer_out.close()


@ray.remote
class _RayRunnerStats:
    def __init__(self):
        self._bundle_uid = 0

    def next_bundle(self):
        self._bundle_uid += 1
        return self._bundle_uid


class RayWorkerHandlerManager:
    def __init__(self):
        self._process_bundle_descriptors = {}

    def register_process_bundle_descriptor(
        self, process_bundle_descriptor: beam_fn_api_pb2.ProcessBundleDescriptor
    ):
        self._process_bundle_descriptors[
            process_bundle_descriptor.id
        ] = process_bundle_descriptor.SerializeToString()

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
            [],  # self.must_follow,
            self.parent,
            self.environment,
            self.forced_root,
        )

        def deserializer(*args):
            return RayStage(
                args[0],
                [beam_runner_api_pb2.PTransform.FromString(s) for s in args[1]],
                args[2],
                args[3],
                args[4],
                args[5],
                args[6],
            )

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
class PcollectionBufferManager:
    def __init__(self):
        self.buffers = collections.defaultdict(list)

    def put(self, pcoll, data_refs: List[ray.ObjectRef]):
        self.buffers[pcoll].extend(data_refs)

    def get(self, pcoll) -> List[ray.ObjectRef]:
        return self.buffers[pcoll]

    def clear(self, pcoll):
        self.buffers[pcoll].clear()


@ray.remote
class RayWatermarkManager(watermark_manager.WatermarkManager):
    def __init__(self):
        # the original WatermarkManager performs a lot of computation
        # in its __init__ method. Because Ray calls __init__ whenever
        # it deserializes an object, we'll move its setup elsewhere.
        self._initialized = False
        self._pcollections_by_name = {}
        self._stages_by_name = {}

    def setup(self, stages):
        if self._initialized:
            return
        logging.debug("initialized the RayWatermarkManager")
        self._initialized = True
        watermark_manager.WatermarkManager.setup(self, stages)


class RayRunnerExecutionContext(object):
    def __init__(
        self,
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
            deserializer=lambda s: beam_runner_api_pb2.Components.FromString(s),
        )
        ray.util.register_serializer(
            pipeline_context.PipelineContext,
            serializer=lambda x: x.proto.SerializeToString(),
            deserializer=lambda s: pipeline_context.PipelineContext(
                proto=beam_runner_api_pb2.Components.FromString(s)
            ),
        )

        self.pcollection_buffers = (
            pcollection_buffers or PcollectionBufferManager.remote()
        )
        self.state_servicer = state_servicer or RayStateManager()
        self.stages = [
            RayStage.from_Stage(s) if not isinstance(s, RayStage) else s for s in stages
        ]
        self.side_input_descriptors_by_stage = (
            fn_execution.FnApiRunnerExecutionContext._build_data_side_inputs_map(stages)
        )
        self.pipeline_components = pipeline_components
        self.safe_coders = safe_coders
        self.data_channel_coders = data_channel_coders

        self.input_transform_to_buffer_id = {
            t.unique_name: bytes(t.spec.payload)
            for s in stages
            for t in s.transforms
            if t.spec.urn == bundle_processor.DATA_INPUT_URN
        }
        self._watermark_manager = RayWatermarkManager.remote()
        self.pipeline_context = pipeline_context.PipelineContext(pipeline_components)
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
        # We don't need to wait for this line to execute with ray.get,
        # because any further calls to the watermark manager actor will
        # have to wait for it.
        self._watermark_manager.setup.remote(self.stages)
        return self._watermark_manager

    @staticmethod
    def next_uid():
        # TODO(pabloem): Use stats actor for UIDs.
        # return str(ray.get(self.stats.next_bundle.remote()))
        # self._uid += 1
        return str(random.randint(0, 11111111))

    def _build_timer_coders_id_map(self):
        from apache_beam.utils import proto_utils

        timer_coder_ids = {}
        for (
            transform_id,
            transform_proto,
        ) in self.pipeline_components.transforms.items():
            if transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
                pardo_payload = proto_utils.parse_Bytes(
                    transform_proto.spec.payload, beam_runner_api_pb2.ParDoPayload
                )
                for id, timer_family_spec in pardo_payload.timer_family_specs.items():
                    timer_coder_ids[
                        (transform_id, id)
                    ] = timer_family_spec.timer_family_coder_id
        return timer_coder_ids

    def __reduce__(self):
        # We need to implement custom serialization for this particular class
        # because it contains several members that are protocol buffers, and
        # protobufs are not pickleable due to being a C extension - so we serialize
        # protobufs to string before serialzing the rest of the object.
        data = (
            self.stages,
            self.pipeline_components.SerializeToString(),
            self.safe_coders,
            self.data_channel_coders,
            self.state_servicer,
            self.worker_manager,
            self.pcollection_buffers,
        )

        def deserializer(*args):
            return RayRunnerExecutionContext(
                args[0],
                beam_runner_api_pb2.Components.FromString(args[1]),
                args[2],
                args[3],
                args[4],
                args[5],
                args[6],
            )

        return (deserializer, data)


def merge_stage_results(
    previous_result: beam_fn_api_pb2.InstructionResponse,
    last_result: beam_fn_api_pb2.InstructionResponse,
) -> beam_fn_api_pb2.InstructionResponse:
    """Merge InstructionResponse objects from executions of same stage bundles.

    This method is used to produce a global per-stage result object with
    aggregated metrics and results.
    """
    return (
        last_result
        if previous_result is None
        else beam_fn_api_pb2.InstructionResponse(
            process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
                monitoring_infos=monitoring_infos.consolidate(
                    itertools.chain(
                        previous_result.process_bundle.monitoring_infos,
                        last_result.process_bundle.monitoring_infos,
                    )
                )
            ),
            error=previous_result.error or last_result.error,
        )
    )


def _get_worker_handler(
    runner_context: RayRunnerExecutionContext, bundle_descriptor_id
) -> worker_handlers.WorkerHandler:
    worker_handler = worker_handlers.EmbeddedWorkerHandler(
        None,  # Unnecessary payload.
        runner_context.state_servicer,
        None,  # Unnecessary provision info.
        runner_context.worker_manager,
    )
    worker_handler.worker.bundle_processor_cache.register(
        runner_context.worker_manager.process_bundle_descriptor(bundle_descriptor_id)
    )
    return worker_handler


@dataclasses.dataclass
class Bundle:
    input_timers: Mapping[translations.TimerFamilyId, fn_execution.PartitionableBuffer]
    input_data: Mapping[str, List[ray.ObjectRef]]
