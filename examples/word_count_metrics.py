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

"""A word-counting workflow."""

# pytype: skip-file

# beam-playground:
#   name: WordCountWithMetrics
#   description: A word-counting workflow with metrics.
#   multifile: false
#   default_example: true
#   pipeline_options: --output output.txt
#   context_line: 48
#   categories:
#     - Combiners
#     - Options
#     - Metrics
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - count
#     - metrics
#     - strings

import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def __init__(self):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super().__init__()
        beam.DoFn.__init__(self)
        self.words_counter = Metrics.counter(self.__class__, "words")
        self.word_lengths_counter = Metrics.counter(self.__class__, "word_lengths")
        self.word_lengths_dist = Metrics.distribution(self.__class__, "word_len_dist")
        self.empty_line_counter = Metrics.counter(self.__class__, "empty_lines")

    def process(self, element):
        """Returns an iterator over the words of this element.

        The element is a line of text.  If the line is blank, note that, too.

        Args:
          element: the element being processed

        Returns:
          The processed element.
        """
        import re

        text_line = element.strip()
        if not text_line:
            self.empty_line_counter.inc(1)
        words = re.findall(r"[\w\']+", text_line, re.UNICODE)
        for w in words:
            self.words_counter.inc()
            self.word_lengths_counter.inc(len(w))
            self.word_lengths_dist.update(len(w))
        return words


def main(argv=None, save_main_session=True, runner=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", dest="input", default="input.txt", help="Input file to process."
    )
    parser.add_argument(
        "--output",
        dest="output",
        default="output.txt",
        help="Output file to write results to.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    if runner == "ray":
        p = beam.Pipeline(runner=ray_runner.RayFnApiRunner(), options=pipeline_options)
    else:
        p = beam.Pipeline(options=pipeline_options)

    # Read the text file[pattern] into a PCollection.
    lines = p | "read" >> ReadFromText(known_args.input)

    # Count the occurrences of each word.
    def count_ones(word_ones):
        (word, ones) = word_ones
        return (word, sum(ones))

    counts = (
        lines
        | "split" >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
        | "pair_with_one" >> beam.Map(lambda x: (x, 1))
        | "group" >> beam.GroupByKey()
        | "count" >> beam.Map(count_ones)
    )

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
        (word, count) = word_count
        return "%s: %d" % (word, count)

    output = counts | "format" >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | "write" >> WriteToText(known_args.output)

    result = p.run()
    result.wait_until_finish()

    # Do not query metrics when creating a template which doesn't run
    if (
        not hasattr(result, "has_job") or result.has_job  # direct runner
    ):  # not just a template creation
        # Query element-wise metrics, e.g., counter, distribution
        empty_lines_filter = MetricsFilter().with_name("empty_lines")
        query_result = result.metrics().query(empty_lines_filter)
        if query_result["counters"]:
            empty_lines_counter = query_result["counters"][0]
            print(f"number of empty lines:{empty_lines_counter.result}")

        word_lengths_filter = MetricsFilter().with_name("word_len_dist")
        query_result = result.metrics().query(word_lengths_filter)
        if query_result["distributions"]:
            word_lengths_dist = query_result["distributions"][0]
            print("average word length:%.2f" % word_lengths_dist.result.mean)
            print(f"min word length: {word_lengths_dist.result.min}")
            print(f"max word length: {word_lengths_dist.result.max}")

        # #Query non-user metrics, e.g., start_bundle_msecs, process_bundle_msecs
        # result_metrics = result.monitoring_metrics()
        # #import pytest
        # #pytest.set_trace()
        # all_metrics_via_monitoring_infos = result_metrics.query()
        # print(all_metrics_via_monitoring_infos['counters'][0].result)
        # print(all_metrics_via_monitoring_infos['counters'][1].result)


if __name__ == "__main__":
    # logging.getLogger().setLevel(logging.DEBUG)
    # Test 1: Ray Runner
    print("Ray Runner--------------->")
    import ray_beam_runner.portability.ray_fn_runner as ray_runner
    import ray

    ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    main(runner="ray")
    ray.shutdown()
    print("Direct Runner--------------->")
    # Test 2: Direct Runner
    main()