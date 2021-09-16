import numpy as np
import pandas as pd


def group_by_key(df: pd.DataFrame):
    # We convert to strings here to support void keys
    if isinstance(df[0].dtype, np.object):
        df[0] = df[0].map(lambda x: str(x))

    groups = {part[0]: list(part[1][1]) for part in df.groupby(0)}
    return groups
