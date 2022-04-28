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

"""A PipelineRunner using the SDK harness.
"""
# pytype: skip-file
# mypy: check-untyped-defs

import copy
import itertools
import logging
from typing import TYPE_CHECKING
from typing import Dict
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.metrics import metric
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.metricbase import MetricName
from apache_beam.options import pipeline_options
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import runner
from apache_beam.runners.common import group_by_key_input_visitor
from apache_beam.runners.portability.fn_api_runner import execution
from apache_beam.runners.portability.fn_api_runner import fn_runner
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner.execution import ListBuffer
from apache_beam.runners.portability.fn_api_runner.translations import create_buffer_id
from apache_beam.runners.portability.fn_api_runner.translations import only_element
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import environments
from apache_beam.utils import timestamp

import ray
from ray_beam_runner.portability.context_management import RayBundleContextManager
from ray_beam_runner.portability.execution import _RayMetricsActor
from ray_beam_runner.portability.execution import ray_execute_bundle
from ray_beam_runner.portability.execution import RayRunnerExecutionContext

if TYPE_CHECKING:
  from apache_beam.pipeline import Pipeline
  from apache_beam.portability.api import metrics_pb2

_LOGGER = logging.getLogger(__name__)

# This module is experimental. No backwards-compatibility guarantees.


class RayFnApiRunner(runner.PipelineRunner):

  NUM_FUSED_STAGES_COUNTER = "__num_fused_stages"

  def __init__(
      self,
      progress_request_frequency: Optional[float] = None,
  ) -> None:

    """Creates a new Ray Runner instance.

    Args:
      progress_request_frequency: The frequency (in seconds) that the runner
          waits before requesting progress from the SDK.
    """
    super().__init__()
    # The default environment is actually a Ray environment.
    self._default_environment = environments.EmbeddedPythonEnvironment.default()
    self._progress_frequency = progress_request_frequency
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
                   pipeline,  # type: Pipeline
                   options  # type: pipeline_options.PipelineOptions
                  ):
    # type: (...) -> RayRunnerResult
    RuntimeValueProvider.set_runtime_options({})

    # Setup "beam_fn_api" experiment options if lacked.
    experiments = (
        options.view_as(pipeline_options.DebugOptions).experiments or [])
    if not 'beam_fn_api' in experiments:
      experiments.append('beam_fn_api')
    options.view_as(pipeline_options.DebugOptions).experiments = experiments

    # This is sometimes needed if type checking is disabled
    # to enforce that the inputs (and outputs) of GroupByKey operations
    # are known to be KVs.
    pipeline.visit(
        group_by_key_input_visitor(
            not options.view_as(pipeline_options.TypeOptions).
            allow_non_deterministic_key_coders))

    self._latest_run_result = self.run_via_runner_api(
        pipeline.to_runner_api(default_environment=self._default_environment))
    return self._latest_run_result

  def run_via_runner_api(self, pipeline_proto):
    # type: (beam_runner_api_pb2.Pipeline) -> RayRunnerResult
    fn_runner.FnApiRunner._validate_requirements(None, pipeline_proto)

    # TODO(pabloem): Implement _check_requirements once RayRunner implements
    #    its own set of features.
    fn_runner.FnApiRunner._check_requirements(self, pipeline_proto)
    stage_context, stages = self.create_stages(pipeline_proto)
    return self.run_stages(stage_context, stages)

  def create_stages(
      self,
      pipeline_proto  # type: beam_runner_api_pb2.Pipeline
  ):
    # type: (...) -> Tuple[translations.TransformContext, List[translations.Stage]]
    return translations.create_and_optimize_stages(
        copy.deepcopy(pipeline_proto),
        phases=[
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

  def run_stages(self,
                 stage_context,  # type: translations.TransformContext
                 stages  # type: List[translations.Stage]
                ):
    # type: (...) -> RayRunnerResult

    """Run a list of topologically-sorted stages in batch mode.

    Args:
      stage_context (translations.TransformContext)
      stages (list[fn_api_runner.translations.Stage])
    """
    pipeline_metrics = MetricsContainer('')
    pipeline_metrics.get_counter(
        MetricName(
            str(type(self)),
            self.NUM_FUSED_STAGES_COUNTER,
            urn='internal:' + self.NUM_FUSED_STAGES_COUNTER)).update(
                len(stages))
    monitoring_infos_by_stage = {}

    runner_execution_context = RayRunnerExecutionContext(
        stages,
        stage_context.components,
        stage_context.safe_coders,
        stage_context.data_channel_coders)

    try:
      for stage in stages:
        bundle_context_manager = RayBundleContextManager(
            runner_execution_context, stage)
        # bundle_context_manager._worker_handlers = [
        #     worker_handlers.EmbeddedWorkerHandler(
        #         None, # Unnecessary payload.
        #         runner_execution_context.state_servicer,
        #         None, # Unnecessary provision info.
        #         runner_execution_context.worker_manager,
        #     )
        # ]

        assert (
            runner_execution_context.watermark_manager.get_stage_node(
                bundle_context_manager.stage.name
            ).input_watermark() == timestamp.MAX_TIMESTAMP), (
            'wrong watermark for %s. Expected %s, but got %s.' % (
                runner_execution_context.watermark_manager.get_stage_node(
                    bundle_context_manager.stage.name),
                timestamp.MAX_TIMESTAMP,
                runner_execution_context.watermark_manager.get_stage_node(
                    bundle_context_manager.stage.name
                ).input_watermark()
            )
        )

        stage_results = self._run_stage(
            runner_execution_context, bundle_context_manager)

        assert (
            runner_execution_context.watermark_manager.get_stage_node(
                bundle_context_manager.stage.name
            ).input_watermark() == timestamp.MAX_TIMESTAMP), (
            'wrong input watermark for %s. Expected %s, but got %s.' % (
            runner_execution_context.watermark_manager.get_stage_node(
                bundle_context_manager.stage.name),
            timestamp.MAX_TIMESTAMP,
            runner_execution_context.watermark_manager.get_stage_node(
                bundle_context_manager.stage.name
            ).output_watermark())
        )

        monitoring_infos_by_stage[stage.name] = (
            list(stage_results.process_bundle.monitoring_infos))

      monitoring_infos_by_stage[''] = list(
          pipeline_metrics.to_runner_api_monitoring_infos('').values())
    finally:
      pass
    return RayRunnerResult(runner.PipelineState.DONE, monitoring_infos_by_stage)

  @staticmethod
  def _collect_written_timers(
      bundle_context_manager: execution.BundleContextManager,
      output_buffers: Mapping[bytes, execution.PartitionableBuffer],
  ) -> Tuple[Dict[translations.TimerFamilyId, timestamp.Timestamp],
             Dict[translations.TimerFamilyId, ListBuffer]]:
    """Review output buffers, and collect written timers.

    This function reviews a stage that has just been run. The stage will have
    written timers to its output buffers. The function then takes the timers,
    and adds them to the `newly_set_timers` dictionary, and the
    timer_watermark_data dictionary.

    The function then returns the following two elements in a tuple:
    - timer_watermark_data: A dictionary mapping timer family to upcoming
        timestamp to fire.
    - newly_set_timers: A dictionary mapping timer family to timer buffers
        to be passed to the SDK upon firing.
    """
    timer_watermark_data = {}
    newly_set_timers = {}
    print(list(output_buffers.keys()))
    for (transform_id, timer_family_id) in bundle_context_manager.stage.timers:
      print('transf timerfam ' + str((transform_id, timer_family_id)))
      written_timers = output_buffers.get(
        create_buffer_id(timer_family_id, kind='timers'), None)
      if not written_timers:
        written_timers = execution.ListBuffer(None)
        written_timers.clear()
      assert isinstance(written_timers, ListBuffer)
      timer_coder_impl = bundle_context_manager.get_timer_coder_impl(
          transform_id, timer_family_id)
      if not written_timers.cleared:
        timers_by_key_and_window = {}
        for elements_timers in written_timers:
          for decoded_timer in timer_coder_impl.decode_all(elements_timers):
            timers_by_key_and_window[decoded_timer.user_key,
                                     decoded_timer.windows[0]] = decoded_timer
        out = create_OutputStream()
        for decoded_timer in timers_by_key_and_window.values():
          # Only add not cleared timer to fired timers.
          if not decoded_timer.clear_bit:
            timer_coder_impl.encode_to_stream(decoded_timer, out, True)
            if (transform_id, timer_family_id) not in timer_watermark_data:
              timer_watermark_data[(transform_id,
                                    timer_family_id)] = timestamp.MAX_TIMESTAMP
            timer_watermark_data[(transform_id, timer_family_id)] = min(
                timer_watermark_data[(transform_id, timer_family_id)],
                decoded_timer.hold_timestamp)
        newly_set_timers[(transform_id, timer_family_id)] = ListBuffer(
            coder_impl=timer_coder_impl)
        newly_set_timers[(transform_id, timer_family_id)].append(out.get())
        written_timers.clear()

    return timer_watermark_data, newly_set_timers

  def _add_sdk_delayed_applications_to_deferred_inputs(
      self,
      bundle_context_manager,  # type: execution.BundleContextManager
      bundle_result,  # type: beam_fn_api_pb2.InstructionResponse
      deferred_inputs  # type: MutableMapping[str, execution.PartitionableBuffer]
  ):
    # type: (...) -> Set[str]

    """Returns a set of PCollection IDs of PColls having delayed applications.

    This transform inspects the bundle_context_manager, and bundle_result
    objects, and adds all deferred inputs to the deferred_inputs object.
    """
    pcolls_with_delayed_apps = set()
    for delayed_application in bundle_result.process_bundle.residual_roots:
      producer_name = bundle_context_manager.input_for(
          delayed_application.application.transform_id,
          delayed_application.application.input_id)
      if producer_name not in deferred_inputs:
        deferred_inputs[producer_name] = ListBuffer(
            coder_impl=bundle_context_manager.get_input_coder_impl(
                producer_name))
      deferred_inputs[producer_name].append(
          delayed_application.application.element)

      transform = bundle_context_manager.process_bundle_descriptor.transforms[
          producer_name]
      # We take the output with tag 'out' from the producer transform. The
      # producer transform is a GRPC read, and it has a single output.
      pcolls_with_delayed_apps.add(only_element(transform.outputs.values()))
    return pcolls_with_delayed_apps

  def _add_residuals_and_channel_splits_to_deferred_inputs(
      self,
      splits,  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]
      bundle_context_manager,  # type: execution.BundleContextManager
      last_sent,  # type: Dict[str, execution.PartitionableBuffer]
      deferred_inputs  # type: MutableMapping[str, execution.PartitionableBuffer]
  ):
    # type: (...) -> Tuple[Set[str], Set[str]]

    """Returns a two sets representing PCollections with watermark holds.

    The first set represents PCollections with delayed root applications.
    The second set represents PTransforms with channel splits.
    """

    pcolls_with_delayed_apps = set()
    transforms_with_channel_splits = set()
    prev_stops = {}  # type: Dict[str, int]
    for split in splits:
      for delayed_application in split.residual_roots:
        producer_name = bundle_context_manager.input_for(
            delayed_application.application.transform_id,
            delayed_application.application.input_id)
        if producer_name not in deferred_inputs:
          deferred_inputs[producer_name] = ListBuffer(
              coder_impl=bundle_context_manager.get_input_coder_impl(
                  producer_name))
        deferred_inputs[producer_name].append(
            delayed_application.application.element)
        # We take the output with tag 'out' from the producer transform. The
        # producer transform is a GRPC read, and it has a single output.
        pcolls_with_delayed_apps.add(
            bundle_context_manager.process_bundle_descriptor.
            transforms[producer_name].outputs['out'])
      for channel_split in split.channel_splits:
        coder_impl = bundle_context_manager.get_input_coder_impl(
            channel_split.transform_id)
        # Decode and recode to split the encoded buffer by element index.
        all_elements = list(
            coder_impl.decode_all(
                b''.join(last_sent[channel_split.transform_id])))
        residual_elements = all_elements[
            channel_split.first_residual_element:prev_stops.
            get(channel_split.transform_id, len(all_elements)) + 1]
        if residual_elements:
          transform = (
              bundle_context_manager.process_bundle_descriptor.transforms[
                  channel_split.transform_id])
          assert transform.spec.urn == bundle_processor.DATA_INPUT_URN
          transforms_with_channel_splits.add(transform.unique_name)

          if channel_split.transform_id not in deferred_inputs:
            coder_impl = bundle_context_manager.get_input_coder_impl(
                channel_split.transform_id)
            deferred_inputs[channel_split.transform_id] = ListBuffer(
                coder_impl=coder_impl)
          deferred_inputs[channel_split.transform_id].append(
              coder_impl.encode_all(residual_elements))
        prev_stops[
            channel_split.transform_id] = channel_split.last_primary_element
    return pcolls_with_delayed_apps, transforms_with_channel_splits

  def _run_stage(self,
                 runner_execution_context: RayRunnerExecutionContext,
                 bundle_context_manager,  # type: execution.BundleContextManager
                ) -> beam_fn_api_pb2.InstructionResponse:

    """Run an individual stage.

    Args:
      runner_execution_context: An object containing execution information for
        the pipeline.
      bundle_context_manager (execution.BundleContextManager): A description of
        the stage to execute, and its context.
    """
    data_input, data_output, expected_timer_output = (
        bundle_context_manager.extract_bundle_inputs_and_outputs())
    runner_execution_context.worker_manager.register_process_bundle_descriptor(
        bundle_context_manager.process_bundle_descriptor)
    input_timers = {
    }  # type: Mapping[translations.TimerFamilyId, execution.PartitionableBuffer]

    _LOGGER.info('Running %s', bundle_context_manager.stage.name)

    final_result = None  # type: Optional[beam_fn_api_pb2.InstructionResponse]

    def merge_results(last_result):
      # type: (beam_fn_api_pb2.InstructionResponse) -> beam_fn_api_pb2.InstructionResponse

      """ Merge the latest result with other accumulated results. """
      return (
          last_result
          if final_result is None else beam_fn_api_pb2.InstructionResponse(
              process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
                  monitoring_infos=monitoring_infos.consolidate(
                      itertools.chain(
                          final_result.process_bundle.monitoring_infos,
                          last_result.process_bundle.monitoring_infos))),
              error=final_result.error or last_result.error))

    while True:
      last_result, deferred_inputs, fired_timers, watermark_updates, bundle_outputs = (
          self._run_bundle(
              runner_execution_context,
              bundle_context_manager,
              data_input,
              data_output,
              input_timers,
              expected_timer_output))

      for pc_name, watermark in watermark_updates.items():
        runner_execution_context.watermark_manager.set_pcoll_watermark(
            pc_name, watermark)

      final_result = merge_results(last_result)
      if not deferred_inputs and not fired_timers:
        break
      else:
        assert (runner_execution_context.watermark_manager.get_stage_node(
            bundle_context_manager.stage.name).output_watermark()
                < timestamp.MAX_TIMESTAMP), (
            'wrong timestamp for %s. '
            % runner_execution_context.watermark_manager.get_stage_node(
            bundle_context_manager.stage.name))
        data_input = deferred_inputs
        input_timers = fired_timers

    # Store the required downstream side inputs into state so it is accessible
    # for the worker when it runs bundles that consume this stage's output.
    data_side_input = (
        runner_execution_context.side_input_descriptors_by_stage.get(
            bundle_context_manager.stage.name, {}))
    runner_execution_context.commit_side_inputs_to_state(data_side_input)

    return final_result

  def _run_bundle(
      self,
      runner_execution_context: RayRunnerExecutionContext,
      bundle_context_manager: execution.BundleContextManager,
      data_input: Dict[str, execution.PartitionableBuffer],
      data_output: fn_runner.DataOutput,
      input_timers: Mapping[translations.TimerFamilyId, execution.PartitionableBuffer],
      expected_timer_output: Mapping[translations.TimerFamilyId, bytes],
  ) -> Tuple[beam_fn_api_pb2.InstructionResponse,
             Dict[str, execution.PartitionableBuffer],
             Dict[translations.TimerFamilyId, ListBuffer],
             Dict[Union[str, translations.TimerFamilyId], timestamp.Timestamp],
             Dict[Union[str, translations.TimerFamilyId], execution.PartitionableBuffer]]:
    """Execute a bundle, and return a result object, and deferred inputs."""

    cache_token_generator = fn_runner.FnApiRunner.get_cache_token_generator(static=False)

  # TODO(pabloem): Are there two different IDs? the Bundle ID and PBD ID?
    process_bundle_id = 'bundle_%s' % bundle_context_manager.process_bundle_descriptor.id

    result, output = ray_execute_bundle(
        runner_execution_context,
        bundle_context_manager,
        runner_execution_context.state_servicer,
        data_input,
        input_timers,
        data_output,
        expected_timer_output,
        beam_fn_api_pb2.InstructionRequest(
            instruction_id=process_bundle_id,
            process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
                process_bundle_descriptor_id=bundle_context_manager.process_bundle_descriptor.id,
                cache_tokens=[next(cache_token_generator)]))
    )

    # TODO(pabloem): Add support for splitting of results.

    # Now we collect all the deferred inputs remaining from bundle execution.
    # Deferred inputs can be:
    # - timers
    # - SDK-initiated deferred applications of root elements
    # - Runner-initiated deferred applications of root elements
    deferred_inputs = {}  # type: Dict[str, execution.PartitionableBuffer]

    watermarks_by_transform_and_timer_family, newly_set_timers = (
        self._collect_written_timers(bundle_context_manager, output))

    sdk_pcolls_with_da = self._add_sdk_delayed_applications_to_deferred_inputs(
        bundle_context_manager, result, deferred_inputs)

    runner_pcolls_with_da, transforms_with_channel_splits = (
        self._add_residuals_and_channel_splits_to_deferred_inputs(
            [], bundle_context_manager, data_input, deferred_inputs))

    watermark_updates = fn_runner.FnApiRunner._build_watermark_updates(
        runner_execution_context,
        data_input.keys(),
        expected_timer_output.keys(),
        runner_pcolls_with_da.union(sdk_pcolls_with_da),
        transforms_with_channel_splits,
        watermarks_by_transform_and_timer_family)

    # After collecting deferred inputs, we 'pad' the structure with empty
    # buffers for other expected inputs.
    if deferred_inputs or newly_set_timers:
      # The worker will be waiting on these inputs as well.
      for other_input in data_input:
        if other_input not in deferred_inputs:
          deferred_inputs[other_input] = ListBuffer(
              coder_impl=bundle_context_manager.get_input_coder_impl(
                  other_input))

    return result, deferred_inputs, newly_set_timers, watermark_updates, output


class RayMetrics(metric.MetricResults):
  def __init__(self, step_monitoring_infos, ray_metrics: _RayMetricsActor):
    """Used for querying metrics from the PipelineResult object.

      step_monitoring_infos: Per step metrics specified as MonitoringInfos.
      user_metrics_only: If true, includes user metrics only.
    """
    self._metrics_actor = ray_metrics

  def query(self, filter=None):
    counters = [
        # TODO
    ]
    distributions = [# TODO
    ]
    gauges = [
        # TODO
    ]

    return {
        self.COUNTERS: counters,
        self.DISTRIBUTIONS: distributions,
        self.GAUGES: gauges
    }

  def monitoring_infos(self):
    # type: () -> List[metrics_pb2.MonitoringInfo]
    return [
        # TODO
    ]


class RayRunnerResult(runner.PipelineResult):
  def __init__(self, state, ray_metrics: _RayMetricsActor):
    super().__init__(state)
    self._metrics = ray_metrics

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
