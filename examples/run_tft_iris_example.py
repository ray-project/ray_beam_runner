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

import tempfile

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.direct import DirectRunner
import tensorflow_transform.beam as tft_beam

from ray_beam_runner.ray_runner import RayRunner

from tft_iris_example.preprocessing import (
    Split, create_raw_metadata, analyze_and_transform, write_tfrecords,
    write_transform_artefacts)

INPUT_FILENAME = "tft_iris_example/data/input_data/input_data.csv"
OUTPUT_FILENAME = "tft_iris_example/data/output/preprocessed_data"
OUTPUT_TRANSFORM_FUNCTION_FOLDER = "tft_iris_example/" \
                                   "data/output/transform_artfcts"


def run_transformation_pipeline(raw_input_location, transformed_data_location,
                                transform_artefact_location):
    pipeline_options = PipelineOptions(["--parallelism=1"])
    runner_cls = RayRunner

    # pipeline_options = PipelineOptions()
    # runner_cls = DirectRunner

    with beam.Pipeline(
            runner=runner_cls(), options=pipeline_options) as pipeline:
        with tft_beam.Context(temp_dir=tempfile.mkdtemp()):
            raw_data = (pipeline | beam.io.ReadFromText(
                raw_input_location, skip_header_lines=1)
                        | beam.ParDo(Split()))
            raw_metadata = create_raw_metadata()
            raw_dataset = (raw_data, raw_metadata)
            transformed_dataset, transform_fn = analyze_and_transform(
                raw_dataset)
            # transformed_dataset[0] | beam.Map(print)
            write_tfrecords(transformed_dataset, transformed_data_location)
            write_transform_artefacts(transform_fn,
                                      transform_artefact_location)


if __name__ == "__main__":
    run_transformation_pipeline(
        raw_input_location=INPUT_FILENAME,
        transformed_data_location=OUTPUT_FILENAME,
        transform_artefact_location=OUTPUT_TRANSFORM_FUNCTION_FOLDER)
