import pandas as pd
import ray

from ray_beam_runner.util import group_by_key


class RaySideInput(object):
  def __init__(self, ray_ds: ray.data.Dataset):
    self.ray_ds = ray_ds

  def convert_df(self, df: pd.DataFrame):
    return df


class RayListSideInput(RaySideInput):
  def convert_df(self, df: pd.DataFrame):
    return df[0].values.tolist()


class RayDictSideInput(RaySideInput):
  def convert_df(self, df: pd.DataFrame):
    return dict(df.values.tolist())


class RayMultiMapSideInput(RaySideInput):
  def convert_df(self, df: pd.DataFrame):
    return group_by_key(df)
