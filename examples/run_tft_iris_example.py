import tempfile

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
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

    with beam.Pipeline(
            runner=RayRunner(), options=pipeline_options) as pipeline:
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
