import ray

import pandas as pd
from apache_beam.transforms.window import GlobalWindow, WindowedValue


def group_by_key(ray_ds: ray.data.Dataset):
    df = pd.DataFrame()
    for windowed_value in ray_ds.iter_rows():
        if not isinstance(windowed_value, WindowedValue):
            windowed_value = WindowedValue(windowed_value, 0,
                                           (GlobalWindow(), ))

        # Extract key from windowed value
        key, value = windowed_value.value
        # We convert to strings here to support void keys
        key = str(key) if key is None else key

        df = df.append([[key, value]])

        # for i, window in enumerate(windowed_value.windows):
        #     group_key = (key, window)
        #     print("GROUP KEY", group_key, value)
        #     df = df.append([[group_key, value]])

    # part[0] is the key
    # part[1][1] returns the (windowed) value
    groups = {part[0]: list(part[1][1]) for part in df.groupby(0, sort=False)}
    print("GOT GROUPS", groups)
    return groups
