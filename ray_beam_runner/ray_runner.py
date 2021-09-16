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
"""RayRunner, executing on a Ray cluster.

"""

# pytype: skip-file
import logging

import ray
from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
from ray_beam_runner.collection import CollectionMap
from ray_beam_runner.overrides import _get_overrides
from ray_beam_runner.translator import TranslationExecutor
from apache_beam.runners.runner import PipelineState, PipelineResult

__all__ = ["RayRunner"]

from apache_beam.typehints import Dict

_LOGGER = logging.getLogger(__name__)


class RayRunnerOptions(PipelineOptions):
    """DirectRunner-specific execution options."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--parallelism",
            type=int,
            default=1,
            help="Parallelism for Read/Create operations")


class RayRunner(BundleBasedDirectRunner):
    """Executes a single pipeline on the local machine."""

    @staticmethod
    def is_fnapi_compatible():
        return False

    def run_pipeline(self, pipeline, options):
        """Execute the entire pipeline and returns a RayPipelineResult."""
        runner_options = options.view_as(RayRunnerOptions)

        collection_map = CollectionMap()

        # Override some transforms with custom transforms
        overrides = _get_overrides()
        pipeline.replace_all(overrides)

        # Execute transforms using Ray datasets
        translation_executor = TranslationExecutor(
            collection_map, parallelism=runner_options.parallelism)
        pipeline.visit(translation_executor)

        named_graphs = [
            transform.named_outputs()
            for transform in pipeline.transforms_stack
        ]

        outputs = {}
        for named_outputs in named_graphs:
            outputs.update(named_outputs)

        _LOGGER.info("Running pipeline with RayRunner.")

        result = RayPipelineResult(outputs)

        return result


class RayPipelineResult(PipelineResult):
    def __init__(self, named_outputs: Dict[str, ray.data.Dataset]):
        super(RayPipelineResult, self).__init__(PipelineState.RUNNING)
        self.named_outputs = named_outputs

    def __del__(self):
        if self._state == PipelineState.RUNNING:
            _LOGGER.warning(
                "The RayPipelineResult is being garbage-collected while the "
                "RayRunner is still running the corresponding pipeline. "
                "This may lead to incomplete execution of the pipeline if the "
                "main thread exits before pipeline completion. Consider using "
                "result.wait_until_finish() to wait for completion of "
                "pipeline execution.")

    def wait_until_finish(self, duration=None):
        if not PipelineState.is_terminal(self.state):
            if duration:
                raise NotImplementedError(
                    "RayRunner does not support duration argument.")
            try:
                objs = list(self.named_outputs.values())
                ray.wait(objs, num_returns=objs)
                self._state = PipelineState.DONE
            except Exception:  # pylint: disable=broad-except
                self._state = PipelineState.FAILED
                raise
        return self._state
