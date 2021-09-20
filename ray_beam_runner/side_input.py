import pandas as pd
import ray

from ray_beam_runner.util import group_by_key


class RaySideInput(object):
    def __init__(self, ray_ds: ray.data.Dataset, convert_fn):
        self.ray_ds = ray_ds
        self.convert_fn = convert_fn

    def convert_df(self, df: pd.DataFrame):
        raise NotImplementedError


class RayIterableSideInput(RaySideInput):
    def convert_df(self, df: pd.DataFrame):
        def _native(np_item):
            return np_item.item() if len(np_item) <= 1 else tuple(np_item)

        return self.convert_fn([_native(row) for row in df.to_numpy()])


class RayMultiMapSideInput(RaySideInput):
    def convert_df(self, df: pd.DataFrame):
        return group_by_key(df)
