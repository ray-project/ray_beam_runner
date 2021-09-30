import typing

from apache_beam import (pvalue, PTransform, Create, Reshuffle, Windowing,
                         GroupByKey, ParDo)
from apache_beam.io import Read
from apache_beam.pipeline import PTransformOverride
from apache_beam.runners.direct.direct_runner import _GroupAlsoByWindowDoFn
from apache_beam.transforms.window import GlobalWindows
from apache_beam import typehints
from apache_beam.typehints import List, trivial_inference

K = typing.TypeVar("K")
V = typing.TypeVar("V")


class _Create(PTransform):
    def __init__(self, values):
        self.values = values

    def expand(self, input_or_inputs):
        return pvalue.PCollection.from_(input_or_inputs)

    def get_windowing(self, inputs):
        # type: (typing.Any) -> Windowing
        return Windowing(GlobalWindows())


@typehints.with_input_types(K)
@typehints.with_output_types(K)
class _Reshuffle(PTransform):
    def expand(self, input_or_inputs):
        return pvalue.PCollection.from_(input_or_inputs)


class _Read(PTransform):
    def __init__(self, source):
        self.source = source

    def expand(self, input_or_inputs):
        return pvalue.PCollection.from_(input_or_inputs)


@typehints.with_input_types(typing.Tuple[K, V])
@typehints.with_output_types(typing.Tuple[K, typing.Iterable[V]])
class _GroupByKeyOnly(PTransform):
    def expand(self, input_or_inputs):
        return pvalue.PCollection.from_(input_or_inputs)

    def infer_output_type(self, input_type):
        key_type, value_type = trivial_inference.key_value_types(input_type)
        return typehints.KV[key_type, typehints.Iterable[value_type]]


@typehints.with_input_types(typing.Tuple[K, typing.Iterable[V]])
@typehints.with_output_types(typing.Tuple[K, typing.Iterable[V]])
class _GroupAlsoByWindow(ParDo):
    def __init__(self, windowing):
        super(_GroupAlsoByWindow, self).__init__(
            _GroupAlsoByWindowDoFn(windowing))
        self.windowing = windowing

    def expand(self, input_or_inputs):
        return pvalue.PCollection.from_(input_or_inputs)


@typehints.with_input_types(typing.Tuple[K, V])
@typehints.with_output_types(typing.Tuple[K, typing.Iterable[V]])
class _GroupByKey(PTransform):
    def expand(self, input_or_inputs):
        return (
            input_or_inputs
            | "ReifyWindows" >> ParDo(GroupByKey.ReifyWindows())
            | "GroupByKey" >> _GroupByKeyOnly()
            | "GroupByWindow" >> _GroupAlsoByWindow(input_or_inputs.windowing))


def _get_overrides() -> List[PTransformOverride]:
    class CreateOverride(PTransformOverride):
        def matches(self, applied_ptransform):
            return applied_ptransform.transform.__class__ == Create

        def get_replacement_transform_for_applied_ptransform(
                self, applied_ptransform):
            # Use specialized streaming implementation.
            transform = _Create(applied_ptransform.transform.values)
            return transform

    class ReshuffleOverride(PTransformOverride):
        def matches(self, applied_ptransform):
            return applied_ptransform.transform.__class__ == Reshuffle

        def get_replacement_transform_for_applied_ptransform(
                self, applied_ptransform):
            # Use specialized streaming implementation.
            transform = _Reshuffle()
            return transform

    class ReadOverride(PTransformOverride):
        def matches(self, applied_ptransform):
            return applied_ptransform.transform.__class__ == Read

        def get_replacement_transform_for_applied_ptransform(
                self, applied_ptransform):
            # Use specialized streaming implementation.
            transform = _Read(applied_ptransform.transform.source)
            return transform

    class GroupByKeyOverride(PTransformOverride):
        def matches(self, applied_ptransform):
            return applied_ptransform.transform.__class__ == GroupByKey

        def get_replacement_transform_for_applied_ptransform(
                self, applied_ptransform):
            # Use specialized streaming implementation.
            transform = _GroupByKey()
            return transform

    return [
        CreateOverride(),
        ReshuffleOverride(),
        ReadOverride(),
        GroupByKeyOverride(),
    ]
