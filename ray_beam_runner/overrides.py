from apache_beam import (pvalue, PTransform, Create, Reshuffle)
from apache_beam.io import Read
from apache_beam.pipeline import PTransformOverride
from apache_beam.typehints import List


class _Create(PTransform):
  def __init__(self, values):
    self.values = values

  def expand(self, input_or_inputs):
    return pvalue.PCollection.from_(input_or_inputs)


class _Reshuffle(PTransform):
  def expand(self, input_or_inputs):
    return pvalue.PCollection.from_(input_or_inputs)


class _Read(PTransform):
  def __init__(self, source):
    self.source = source

  def expand(self, input_or_inputs):
    return pvalue.PCollection.from_(input_or_inputs)


def _get_overrides() -> List[PTransformOverride]:
  class CreateOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      # Note: we match the exact class, since we replace it with a subclass.
      return applied_ptransform.transform.__class__ == Create

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      # Use specialized streaming implementation.
      transform = _Create(applied_ptransform.transform.values)
      return transform

  class ReshuffleOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      # Note: we match the exact class, since we replace it with a subclass.
      return applied_ptransform.transform.__class__ == Reshuffle

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      # Use specialized streaming implementation.
      transform = _Reshuffle()
      return transform

  class ReadOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      # Note: we match the exact class, since we replace it with a subclass.
      return applied_ptransform.transform.__class__ == Read

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      # Use specialized streaming implementation.
      transform = _Read(applied_ptransform.transform.source)
      return transform

  return [
    CreateOverride(),
    ReshuffleOverride(),
    ReadOverride(),
  ]
