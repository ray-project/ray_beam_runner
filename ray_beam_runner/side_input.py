import pandas as pd
import ray

from ray_beam_runner.util import group_by_key


class RaySideInput(object):
    def __init__(self, ray_ds: ray.data.Dataset, convert_fn):
        self.ray_ds = ray_ds
        self.convert_fn = convert_fn

    def convert(self):
        raise NotImplementedError


class RayIterableSideInput(RaySideInput):
    def convert(self):
        return self.convert_fn(self.ray_ds.iter_rows())


class RayMultiMapSideInput(RaySideInput):
    def convert(self):
        df = ray.get(self.ray_ds.to_pandas())
        return group_by_key(df)
