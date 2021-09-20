# flake8: noqa
import pprint
import tempfile
import unicodedata
import os

import tensorflow as tf
import tensorflow_transform as tft
# import numpy as np
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import tensorflow_transform.beam as tft_beam
from tensorflow_transform.tf_metadata import dataset_metadata
# from tensorflow_transform.tf_metadata import dataset_schema
from tensorflow_transform.tf_metadata import schema_utils

INPUT_FILENAME = 'data/input/input_data.csv'
OUTPUT_FILENAME = 'data/output/preprocessed_data'
OUTPUT_TRANSFORM_FUNCTION_FOLDER = 'data/output/transform_artfcts'

NUMERIC_FEATURE_KEYS = [
    'sepal_length', 'sepal_width', 'petal_length', 'petal_width'
]

LABEL_KEY = 'target'


def create_raw_metadata():
    listFeatures = [(name, tf.io.FixedLenFeature([1], tf.float32))
                    for name in NUMERIC_FEATURE_KEYS
                    ] + [(LABEL_KEY, tf.io.VarLenFeature(tf.string))]
    RAW_DATA_FEATURE_SPEC = dict(listFeatures)

    RAW_DATA_METADATA = tft.tf_metadata.dataset_metadata.DatasetMetadata(
        schema_utils.schema_from_feature_spec(RAW_DATA_FEATURE_SPEC))
    return RAW_DATA_METADATA


class Split(beam.DoFn):
    def process(self, element):
        import numpy as np
        sepal_length, sepal_width, petal_length, petal_width, target = element.split(
            ",")
        return [{
            "sepal_length": np.array([float(sepal_length)]),
            "sepal_width": np.array([float(sepal_width)]),
            "petal_length": np.array([float(petal_length)]),
            "petal_width": np.array([float(petal_width)]),
            "target": target,
        }]


def preprocess_fn(input_features):
    output_features = {}

    # Target feature
    # This is a SparseTensor because it is optional. Here we fill in a default
    # value when it is missing. This is useful when this column is missing during
    # inference
    sparse = tf.sparse.SparseTensor(
        indices=input_features[LABEL_KEY].indices,
        values=input_features[LABEL_KEY].values,
        dense_shape=[input_features[LABEL_KEY].dense_shape[0], 1])
    dense = tf.sparse.to_dense(sp_input=sparse, default_value='')
    #         # Reshaping from a batch of vectors of size 1 to a batch to scalars.
    dense = tf.squeeze(dense, axis=1)
    dense_integerized = tft.compute_and_apply_vocabulary(
        dense, vocab_filename="label_index_map")
    output_features['target'] = tf.one_hot(dense_integerized, depth=3)

    # normalization of continuous variables
    output_features['sepal_length_normalized'] = tft.scale_to_z_score(
        input_features['sepal_length'])
    output_features['sepal_width_normalized'] = tft.scale_to_z_score(
        input_features['sepal_width'])
    output_features['petal_length_normalized'] = tft.scale_to_z_score(
        input_features['petal_length'])
    output_features['petal_width_normalized'] = tft.scale_to_z_score(
        input_features['petal_width'])
    return output_features


def analyze_and_transform(raw_dataset, step="Default"):
    transformed_dataset, transform_fn = raw_dataset | "{} - Analyze & Transform".format(
        step) >> tft_beam.AnalyzeAndTransformDataset(preprocess_fn)

    return transformed_dataset, transform_fn


def write_tfrecords(dataset, location, step="Default"):
    transformed_data, transformed_metadata = dataset
    (transformed_data
     | "{} - Write Transformed Data".format(step) >>
     beam.io.tfrecordio.WriteToTFRecord(
         file_path_prefix=os.path.join(location, "{}-".format(step)),
         file_name_suffix=".tfrecords",
         coder=tft.coders.example_proto_coder.ExampleProtoCoder(
             transformed_metadata.schema),
     ))


def write_transform_artefacts(transform_fn, location):
    (transform_fn | "Write Transform Artifacts" >>
     tft_beam.tft_beam_io.transform_fn_io.WriteTransformFn(location))


def run_transformation_pipeline(raw_input_location, transformed_data_location,
                                transform_artefact_location):
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as pipeline:
        with tft_beam.Context(temp_dir=tempfile.mkdtemp()):
            raw_data = (pipeline | beam.io.ReadFromText(
                raw_input_location, skip_header_lines=True)
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
