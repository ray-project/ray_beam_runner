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

from apache_beam.coders.coder_impl import create_OutputStream
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
from apache_beam.utils import proto_utils, timestamp

import ray
from ray_beam_runner.portability.context_management import RayBundleContextManager
from ray_beam_runner.portability.execution import Bundle, _get_input_id
from ray_beam_runner.portability.execution import (
    ray_execute_bundle,
    merge_stage_results,
)
from ray_beam_runner.portability.execution import RayRunnerExecutionContext

_LOGGER = logging.getLogger(__name__)

# This module is experimental. No backwards-compatibility guarantees.


def _setup_options(options: pipeline_options.PipelineOptions):
    """Perform any necessary checkups and updates to input pipeline options"""

    # TODO(pabloem): Add input pipeline options
    RuntimeValueProvider.set_runtime_options({})

    experiments = options.view_as(pipeline_options.DebugOptions).experiments or []
    if "beam_fn_api" not in experiments:
        experiments.append("beam_fn_api")
    options.view_as(pipeline_options.DebugOptions).experiments = experiments


def _check_supported_requirements(
    pipeline_proto: beam_runner_api_pb2.Pipeline,
    supported_requirements: typing.Iterable[str],
):
    """Check that the input pipeline does not have unsuported requirements."""
    for requirement in pipeline_proto.requirements:
        if requirement not in supported_requirements:
            raise ValueError(
                "Unable to run pipeline with requirement: %s" % requirement
            )
    for transform in pipeline_proto.components.transforms.values():
        if transform.spec.urn == common_urns.primitives.TEST_STREAM.urn:
            raise NotImplementedError(transform.spec.urn)
        elif transform.spec.urn in translations.PAR_DO_URNS:
            payload = proto_utils.parse_Bytes(
                transform.spec.payload, beam_runner_api_pb2.ParDoPayload
            )
            for timer in payload.timer_family_specs.values():
                if timer.time_domain != beam_runner_api_pb2.TimeDomain.EVENT_TIME:
                    raise NotImplementedError(timer.time_domain)


def _pipeline_checks(
    pipeline: Pipeline,
    options: pipeline_options.PipelineOptions,
    supported_requirements: typing.Iterable[str],
):
    # This is sometimes needed if type checking is disabled
    # to enforce that the inputs (and outputs) of GroupByKey operations
    # are known to be KVs.
    pipeline.visit(
        group_by_key_input_visitor(
            not options.view_as(
                pipeline_options.TypeOptions
            ).allow_non_deterministic_key_coders
        )
    )

    pipeline_proto = pipeline.to_runner_api(
        default_environment=environments.EmbeddedPythonEnvironment.default()
    )
    fn_runner.FnApiRunner._validate_requirements(None, pipeline_proto)

    _check_supported_requirements(pipeline_proto, supported_requirements)
    return pipeline_proto


class RayFnApiRunner(runner.PipelineRunner):
    def __init__(
        self,
        is_drain=False,
    ) -> None:

        """Creates a new Ray Runner instance.

        Args:
          progress_request_frequency: The frequency (in seconds) that the runner
              waits before requesting progress from the SDK.
          is_drain: identify whether expand the sdf graph in the drain mode.
        """
        super().__init__()
        # TODO: figure out if this is necessary (probably, later)
        self._progress_frequency = None
        self._cache_token_generator = fn_runner.FnApiRunner.get_cache_token_generator()
        self._is_drain = is_drain

    @staticmethod
    def supported_requirements():
        # type: () -> Tuple[str, ...]
        return (
            common_urns.requirements.REQUIRES_STATEFUL_PROCESSING.urn,
            common_urns.requirements.REQUIRES_BUNDLE_FINALIZATION.urn,
            common_urns.requirements.REQUIRES_SPLITTABLE_DOFN.urn,
        )

    def run_pipeline(
        self, pipeline: Pipeline, options: pipeline_options.PipelineOptions
    ) -> "RayRunnerResult":

        # Checkup and set up input pipeline options
        _setup_options(options)

        # Check pipeline and convert into protocol buffer representation
        pipeline_proto = _pipeline_checks(
            pipeline, options, self.supported_requirements()
        )

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
            known_runner_urns=frozenset(
                [
                    common_urns.primitives.FLATTEN.urn,
                    common_urns.primitives.GROUP_BY_KEY.urn,
                ]
            ),
            use_state_iterables=False,
            is_drain=self._is_drain,
        )
        return self.execute_pipeline(stage_context, stages)

    def execute_pipeline(
        self,
        stage_context: translations.TransformContext,
        stages: List[translations.Stage],
    ) -> "RayRunnerResult":
        """Execute pipeline represented by a list of stages and a context."""
        logging.info("Starting pipeline of %d stages." % len(stages))

        runner_execution_context = RayRunnerExecutionContext(
            stages,
            stage_context.components,
            stage_context.safe_coders,
            stage_context.data_channel_coders,
        )

        # Using this queue to hold 'bundles' that are ready to be processed
        queue = collections.deque()

        try:
            for stage in stages:
                bundle_ctx = RayBundleContextManager(runner_execution_context, stage)
                self._run_stage(runner_execution_context, bundle_ctx, queue)
        finally:
            pass
        return RayRunnerResult(runner.PipelineState.DONE)

    def _run_stage(
        self,
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
            bundle_context_manager.process_bundle_descriptor
        )
        input_timers: Mapping[
            translations.TimerFamilyId, execution.PartitionableBuffer
        ] = {}

        input_data = {
            k: runner_execution_context.pcollection_buffers.get(
                _get_input_id(bundle_context_manager.transform_to_buffer_coder[k][0], k)
            )
            for k in bundle_context_manager.transform_to_buffer_coder
        }

        watermark_manager = runner_execution_context.watermark_manager

        final_result = None  # type: Optional[beam_fn_api_pb2.InstructionResponse]

        while True:
            (
                last_result,
                fired_timers,
                delayed_applications,
                bundle_outputs,
            ) = self._run_bundle(
                runner_execution_context,
                bundle_context_manager,
                Bundle(input_timers=input_timers, input_data=input_data),
            )

            final_result = merge_stage_results(final_result, last_result)
            if not delayed_applications and not fired_timers:
                # Processing has completed; marking all outputs as completed
                # TODO: why is it necessary to set both the watermark and
                # the produced_watermark? How do they interact?
                for output_pc in bundle_outputs:
                    _, update_output_pc = translations.split_buffer_id(output_pc)
                    watermark_manager.set_pcoll_produced_watermark.remote(
                        update_output_pc, timestamp.MAX_TIMESTAMP
                    )
                break
            else:
                assert (
                    ray.get(
                        watermark_manager.get_stage_node.remote(
                            bundle_context_manager.stage.name
                        )
                    ).output_watermark()
                    < timestamp.MAX_TIMESTAMP
                ), "wrong timestamp for %s. " % ray.get(
                    watermark_manager.get_stage_node.remote(
                        bundle_context_manager.stage.name
                    )
                )
                input_data = delayed_applications
                input_timers = fired_timers

        # Store the required downstream side inputs into state so it is accessible
        # for the worker when it runs bundles that consume this stage's output.
        data_side_input = runner_execution_context.side_input_descriptors_by_stage.get(
            bundle_context_manager.stage.name, {}
        )
        runner_execution_context.commit_side_inputs_to_state(data_side_input)

        # assert that the output watermark was correctly set for this stage
        stage_node = ray.get(
            runner_execution_context.watermark_manager.get_stage_node.remote(
                bundle_context_manager.stage.name
            )
        )
        assert (
            stage_node.output_watermark() == timestamp.MAX_TIMESTAMP
        ), "wrong output watermark for %s. Expected %s, but got %s." % (
            stage_node,
            timestamp.MAX_TIMESTAMP,
            stage_node.output_watermark(),
        )

        return final_result

    def _run_bundle(
        self,
        runner_execution_context: RayRunnerExecutionContext,
        bundle_context_manager: RayBundleContextManager,
        input_bundle: Bundle,
    ) -> Tuple[
        beam_fn_api_pb2.InstructionResponse,
        Dict[translations.TimerFamilyId, ListBuffer],
        Mapping[str, ray.ObjectRef],
        List[Union[str, translations.TimerFamilyId]],
    ]:
        """Execute a bundle, and return a result object, and deferred inputs."""
        (
            transform_to_buffer_coder,
            data_output,
            stage_timers,
        ) = bundle_context_manager.get_bundle_inputs_and_outputs()

        cache_token_generator = fn_runner.FnApiRunner.get_cache_token_generator(
            static=False
        )

        process_bundle_descriptor = bundle_context_manager.process_bundle_descriptor

        # TODO(pabloem): Are there two different IDs? the Bundle ID and PBD ID?
        process_bundle_id = "bundle_%s" % process_bundle_descriptor.id

        pbd_id = process_bundle_descriptor.id
        result_generator_ref = ray_execute_bundle.remote(
            runner_execution_context,
            input_bundle,
            transform_to_buffer_coder,
            data_output,
            stage_timers,
            instruction_request_repr={
                "instruction_id": process_bundle_id,
                "process_descriptor_id": pbd_id,
                "cache_token": next(cache_token_generator),
            },
        )
        result_generator = iter(ray.get(result_generator_ref))
        result = beam_fn_api_pb2.InstructionResponse.FromString(
            ray.get(next(result_generator))
        )

        output = []
        num_outputs = ray.get(next(result_generator))
        for _ in range(num_outputs):
            pcoll = ray.get(next(result_generator))
            data_ref = next(result_generator)
            output.append(pcoll)
            runner_execution_context.pcollection_buffers.put(pcoll, [data_ref])

        delayed_applications = {}
        num_delayed_applications = ray.get(next(result_generator))
        for _ in range(num_delayed_applications):
            pcoll = ray.get(next(result_generator))
            data_ref = next(result_generator)
            delayed_applications[pcoll] = data_ref
            runner_execution_context.pcollection_buffers.put(pcoll, [data_ref])

        (
            watermarks_by_transform_and_timer_family,
            newly_set_timers,
        ) = self._collect_written_timers(bundle_context_manager)

        # TODO(pabloem): Add support for splitting of results.

        # After collecting deferred inputs, we 'pad' the structure with empty
        # buffers for other expected inputs.
        # if deferred_inputs or newly_set_timers:
        #   # The worker will be waiting on these inputs as well.
        #   for other_input in data_input:
        #     if other_input not in deferred_inputs:
        #       deferred_inputs[other_input] = ListBuffer(
        #           coder_impl=bundle_context_manager.get_input_coder_impl(
        #               other_input))

        # TODO: fill expected timers and pcolls with da
        watermark_updates = fn_runner.FnApiRunner._build_watermark_updates(
            runner_execution_context,
            transform_to_buffer_coder.keys(),
            bundle_context_manager.stage_timers.keys(),  # expected_timers
            set(),  # pcolls_with_da
            delayed_applications.keys(),
            watermarks_by_transform_and_timer_family,
        )

        for pc_name, watermark in watermark_updates.items():
            runner_execution_context.watermark_manager.set_pcoll_watermark.remote(
                pc_name, watermark
            )

        return result, newly_set_timers, delayed_applications, output

    @staticmethod
    def _collect_written_timers(
        bundle_context_manager: RayBundleContextManager,
    ) -> Tuple[
        Dict[translations.TimerFamilyId, timestamp.Timestamp],
        Mapping[translations.TimerFamilyId, execution.PartitionableBuffer],
    ]:
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

        execution_context = bundle_context_manager.execution_context
        buffer_manager = execution_context.pcollection_buffers

        for (
            transform_id,
            timer_family_id,
        ), buffer_id in bundle_context_manager.stage_timers.items():
            timer_buffer = buffer_manager.get(buffer_id)

            coder_id = bundle_context_manager._timer_coder_ids[
                (transform_id, timer_family_id)
            ]

            coder = execution_context.pipeline_context.coders[coder_id]
            timer_coder_impl = coder.get_impl()

            timers_by_key_tag_and_window = {}
            if len(timer_buffer) >= 1:
                written_timers = ray.get(timer_buffer[0])
                # clear the timer buffer
                buffer_manager.clear(buffer_id)

                # deduplicate updates to the same timer
                for elements_timers in written_timers:
                    for decoded_timer in timer_coder_impl.decode_all(elements_timers):
                        key_tag_win = (
                            decoded_timer.user_key,
                            decoded_timer.dynamic_timer_tag,
                            decoded_timer.windows[0],
                        )
                        if not decoded_timer.clear_bit:
                            timers_by_key_tag_and_window[key_tag_win] = decoded_timer
                        elif (
                            decoded_timer.clear_bit
                            and key_tag_win in timers_by_key_tag_and_window
                        ):
                            del timers_by_key_tag_and_window[key_tag_win]
            if not timers_by_key_tag_and_window:
                continue

            out = create_OutputStream()
            for decoded_timer in timers_by_key_tag_and_window.values():
                timer_coder_impl.encode_to_stream(decoded_timer, out, True)
                timer_watermark_data[(transform_id, timer_family_id)] = min(
                    timer_watermark_data.get(
                        (transform_id, timer_family_id), timestamp.MAX_TIMESTAMP
                    ),
                    decoded_timer.hold_timestamp,
                )

            buf = ListBuffer(coder_impl=timer_coder_impl)
            buf.append(out.get())
            newly_set_timers[(transform_id, timer_family_id)] = buf
        return timer_watermark_data, newly_set_timers


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
