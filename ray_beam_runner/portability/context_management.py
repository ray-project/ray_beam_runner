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

from typing import List
from typing import Optional

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability.fn_api_runner import execution as fn_execution
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner import worker_handlers
from apache_beam.runners.worker import bundle_processor
from apache_beam.utils import proto_utils

from ray_beam_runner.portability.execution import RayRunnerExecutionContext

class RayBundleContextManager(fn_execution.BundleContextManager):

  def __init__(self,
      execution_context: RayRunnerExecutionContext,
      stage: translations.Stage,
  ) -> None:
    super(RayBundleContextManager, self).__init__(execution_context, stage, None)

  @property
  def worker_handlers(self) -> List[worker_handlers.WorkerHandler]:
    return []

  def data_api_service_descriptor(self) -> Optional[endpoints_pb2.ApiServiceDescriptor]:
    return endpoints_pb2.ApiServiceDescriptor(url='fake')

  def state_api_service_descriptor(self) -> Optional[endpoints_pb2.ApiServiceDescriptor]:
    return None

  @property
  def process_bundle_descriptor(self):
    # type: () -> beam_fn_api_pb2.ProcessBundleDescriptor
    if self._process_bundle_descriptor is None:
      self._process_bundle_descriptor = self._build_process_bundle_descriptor()
      self._timer_coder_ids = self._build_timer_coders_id_map()
    return self._process_bundle_descriptor

  def extract_bundle_inputs_and_outputs(self):
    # type: () -> Tuple[Dict[str, PartitionableBuffer], DataOutput, Dict[TimerFamilyId, bytes]]

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
    data_input = {}  # type: Dict[str, PartitionableBuffer]
    data_output = {}  # type: DataOutput
    # A mapping of {(transform_id, timer_family_id) : buffer_id}
    expected_timer_output = {}  # type: OutputTimers
    for transform in self.stage.transforms:
      if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                bundle_processor.DATA_OUTPUT_URN):
        pcoll_id = transform.spec.payload
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          coder_id = self.execution_context.data_channel_coders[translations.only_element(
              transform.outputs.values())]
          coder = self.execution_context.pipeline_context.coders[
            self.execution_context.safe_coders.get(coder_id, coder_id)]
          if pcoll_id == translations.IMPULSE_BUFFER:
            data_input[transform.unique_name] = fn_execution.ListBuffer(
                coder_impl=coder.get_impl())
            data_input[transform.unique_name].append(fn_execution.ENCODED_IMPULSE_VALUE)
          else:
            # TODO(pabloem): We need to retrieve the input data from data.
            data_input[transform.unique_name] = fn_execution.ListBuffer(
                coder_impl=coder.get_impl())
        elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          data_output[transform.unique_name] = pcoll_id
          coder_id = self.execution_context.data_channel_coders[translations.only_element(
              transform.inputs.values())]
        else:
          raise NotImplementedError
        data_spec = beam_fn_api_pb2.RemoteGrpcPort(coder_id=coder_id)
        data_spec.api_service_descriptor.url = 'fake'
        transform.spec.payload = data_spec.SerializeToString()
      elif transform.spec.urn in translations.PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for timer_family_id in payload.timer_family_specs.keys():
          expected_timer_output[(transform.unique_name, timer_family_id)] = (
              translations.create_buffer_id(timer_family_id, 'timers'))
    return data_input, data_output, expected_timer_output