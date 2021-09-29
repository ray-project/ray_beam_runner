# flake8: noqa
import tensorflow as tf
import tensorflow_transform as tft
from functools import partial
import os
import numpy as np
from tensorflow.keras.layers import Input, Dense
from tensorflow.keras import Model

# VARIANT = "with_actors"
VARIANT = "with_tasks"
# VARIANT = "with_directrunner"

TFT_OUTPUT_DIRECTORY = f"data/output/{VARIANT}/transform_artfcts/"
TFRECORD_PATH = f"data/output/{VARIANT}/preprocessed_data/"


def _parse_function(proto, fs):
    parsed_features = tf.io.parse_single_example(proto, fs)
    return [
        parsed_features["petal_length_normalized"],
        parsed_features["petal_width_normalized"],
        parsed_features["sepal_length_normalized"],
        parsed_features["sepal_width_normalized"]
    ], parsed_features["target"]


def get_parse_function(tft_output_dir):
    tf_transform_output = tft.TFTransformOutput(tft_output_dir)
    feature_spec = tf_transform_output.transformed_feature_spec()
    parse_func = lambda proto: _parse_function(proto=proto, fs=feature_spec)
    return parse_func


def load_dataset(input_path, tft_output_dir, batch_size=10, shuffle_buffer=2):
    _parse_fn = get_parse_function(tft_output_dir=tft_output_dir)
    filenames = [
        os.path.join(input_path, x) for x in tf.io.gfile.listdir(input_path)
    ]
    dataset = tf.data.TFRecordDataset(filenames)
    dataset = dataset.map(_parse_fn)
    dataset = dataset.batch(batch_size)
    return dataset


dataset = load_dataset(
    input_path=TFRECORD_PATH, tft_output_dir=TFT_OUTPUT_DIRECTORY)


def build_model(input_shape=4, validation_split=0.1):
    tf.keras.backend.one_hot
    input_layer = Input(shape=(4, ))
    hidden_layer = Dense(64, activation="relu")
    hidden_output = hidden_layer(input_layer)
    output_layer = Dense(3, activation="softmax")
    output = output_layer(input_layer)
    model = Model(inputs=input_layer, outputs=output)
    model.compile(
        loss="categorical_crossentropy", optimizer="adam", metrics="accuracy")
    return model


model = build_model()
_ = model.fit(dataset, epochs=100, verbose=False)

input_args = {
    "sepal_length": np.array([[5.1]]),
    "sepal_width": np.array([[3.5]]),
    "petal_length": np.array([[1.4]]),
    "petal_width": np.array([[0.2]]),
}

signature_dict = {
    "petal_width": [
        tf.TensorSpec(shape=[], dtype=tf.float32, name="petal_width")
    ],
    "petal_length": [
        tf.TensorSpec(shape=[], dtype=tf.float32, name="petal_length")
    ],
    "sepal_width": [
        tf.TensorSpec(shape=[], dtype=tf.float32, name="sepal_width")
    ],
    "sepal_length": [
        tf.TensorSpec(shape=[], dtype=tf.float32, name="sepal_length")
    ],
}


class MyModel(tf.keras.Model):
    def __init__(self, model, tft_output_dir):
        super(MyModel, self).__init__()
        self.model = model
        self.tft_layer = tft.TFTransformOutput(
            tft_output_dir).transform_features_layer()

    def call(self, inputs):
        transformed = self.tft_layer(inputs)

        modelInput = tf.stack(
            [
                transformed["petal_length_normalized"],
                transformed["petal_width_normalized"],
                transformed["sepal_length_normalized"],
                transformed["sepal_width_normalized"]
            ],
            axis=1)
        pred = self.model(modelInput)
        return {"prediction": pred}


myModel = MyModel(model, TFT_OUTPUT_DIRECTORY)

myModel.predict(input_args)

myModel.save(f"data/output/{VARIANT}/saved_model")
