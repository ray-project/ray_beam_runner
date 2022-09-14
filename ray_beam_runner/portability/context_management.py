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
import typing
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability.fn_api_runner import execution as fn_execution
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner import worker_handlers
from apache_beam.runners.portability.fn_api_runner.execution import PartitionableBuffer
from apache_beam.runners.portability.fn_api_runner.fn_runner import OutputTimers
from apache_beam.runners.portability.fn_api_runner.translations import DataOutput
from apache_beam.runners.portability.fn_api_runner.translations import TimerFamilyId
from apache_beam.runners.worker import bundle_processor
from apache_beam.utils import proto_utils

from ray_beam_runner.portability.execution import RayRunnerExecutionContext


class RayBundleContextManager:
    def __init__(
        self,
        execution_context: RayRunnerExecutionContext,
        stage: translations.Stage,
    ) -> None:
        self.execution_context = execution_context
        self.stage = stage
        # self.extract_bundle_inputs_and_outputs()
        self.bundle_uid = self.execution_context.next_uid()

        # Properties that are lazily initialized
        self._process_bundle_descriptor = (
            None
        )  # type: Optional[beam_fn_api_pb2.ProcessBundleDescriptor]
        self._worker_handlers = (
            None
        )  # type: Optional[List[worker_handlers.WorkerHandler]]
        # a mapping of {(transform_id, timer_family_id): timer_coder_id}. The map
        # is built after self._process_bundle_descriptor is initialized.
        # This field can be used to tell whether current bundle has timers.
        self._timer_coder_ids = None  # type: Optional[Dict[Tuple[str, str], str]]

    def __reduce__(self):
        data = (self.execution_context, self.stage)

        def deserializer(args):
            RayBundleContextManager(args[0], args[1])

        return (deserializer, data)

    @property
    def worker_handlers(self) -> List[worker_handlers.WorkerHandler]:
        return []

    def data_api_service_descriptor(
        self,
    ) -> Optional[endpoints_pb2.ApiServiceDescriptor]:
        return endpoints_pb2.ApiServiceDescriptor(url="fake")

    def state_api_service_descriptor(
        self,
    ) -> Optional[endpoints_pb2.ApiServiceDescriptor]:
        return None

    @property
    def process_bundle_descriptor(self) -> beam_fn_api_pb2.ProcessBundleDescriptor:
        if self._process_bundle_descriptor is None:
            self._process_bundle_descriptor = (
                beam_fn_api_pb2.ProcessBundleDescriptor.FromString(
                    self._build_process_bundle_descriptor()
                )
            )
            self._timer_coder_ids = (
                fn_execution.BundleContextManager._build_timer_coders_id_map(self)
            )
        return self._process_bundle_descriptor

    def _build_process_bundle_descriptor(self):
        # Cannot be invoked until *after* _extract_endpoints is called.
        # Always populate the timer_api_service_descriptor.
        pbd = beam_fn_api_pb2.ProcessBundleDescriptor(
            id=self.bundle_uid,
            transforms={
                transform.unique_name: transform for transform in self.stage.transforms
            },
            pcollections=dict(
                self.execution_context.pipeline_components.pcollections.items()
            ),
            coders=dict(self.execution_context.pipeline_components.coders.items()),
            windowing_strategies=dict(
                self.execution_context.pipeline_components.windowing_strategies.items()
            ),
            environments=dict(
                self.execution_context.pipeline_components.environments.items()
            ),
            state_api_service_descriptor=self.state_api_service_descriptor(),
            timer_api_service_descriptor=self.data_api_service_descriptor(),
        )

        return pbd.SerializeToString()

    def get_bundle_inputs_and_outputs(
        self,
    ) -> Tuple[Dict[str, PartitionableBuffer], DataOutput, Dict[TimerFamilyId, bytes]]:
        """Returns maps of transform names to PCollection identifiers.

        Also mutates IO stages to point to the data ApiServiceDescriptor.

        Returns:
          A tuple of (data_input, data_output, expected_timer_output) dictionaries.
            `data_input` is a dictionary mapping (transform_name, output_name) to a
            PCollection buffer; `data_output` is a dictionary mapping
            (transform_name, output_name) to a PCollection ID.
            `expected_timer_output` is a dictionary mapping transform_id and
            timer family ID to a buffer id for timers.
        """
        return self.transform_to_buffer_coder, self.data_output, self.stage_timers

    def setup(self):
        transform_to_buffer_coder: typing.Dict[str, typing.Tuple[bytes, str]] = {}
        data_output = {}  # type: DataOutput
        expected_timer_output = {}  # type: OutputTimers
        for transform in self.stage.transforms:
            if transform.spec.urn in (
                bundle_processor.DATA_INPUT_URN,
                bundle_processor.DATA_OUTPUT_URN,
            ):
                pcoll_id = transform.spec.payload
                if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
                    coder_id = self.execution_context.data_channel_coders[
                        translations.only_element(transform.outputs.values())
                    ]
                    if pcoll_id == translations.IMPULSE_BUFFER:
                        pcoll_id = transform.unique_name.encode("utf8")
                        self.execution_context.pcollection_buffers.put(
                            pcoll_id, [self.execution_context.encoded_impulse_ref]
                        )
                    else:
                        pass
                    transform_to_buffer_coder[transform.unique_name] = (
                        pcoll_id,
                        self.execution_context.safe_coders.get(coder_id, coder_id),
                    )
                elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
                    data_output[transform.unique_name] = pcoll_id
                    coder_id = self.execution_context.data_channel_coders[
                        translations.only_element(transform.inputs.values())
                    ]
                else:
                    raise NotImplementedError
                data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
                transform.spec.payload = data_spec.SerializeToString()
            elif transform.spec.urn in translations.PAR_DO_URNS:
                payload = proto_utils.parse_Bytes(
                    transform.spec.payload, beam_runner_api_pb2.ParDoPayload
                )
                for timer_family_id in payload.timer_family_specs.keys():
                    expected_timer_output[
                        (transform.unique_name, timer_family_id)
                    ] = translations.create_buffer_id(timer_family_id, "timers")
        self.transform_to_buffer_coder, self.data_output, self.stage_timers = (
            transform_to_buffer_coder,
            data_output,
            expected_timer_output,
        )
