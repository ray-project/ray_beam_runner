import ray

import pandas as pd
from apache_beam.transforms.window import GlobalWindow, WindowedValue
from apache_beam.pipeline import PipelineVisitor


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

    if df.empty:
        return {}

    # part[0] is the key
    # part[1][1] returns the (windowed) value
    groups = {part[0]: list(part[1][1]) for part in df.groupby(0, sort=False)}
    return groups


class PipelinePrinter(PipelineVisitor):
    def visit_value(self, value, producer_node):
        print(f"visit_value(value, {producer_node.full_label})")

    def visit_transform(self, transform_node):
        print(f"visit_transform({type(transform_node.transform)})")

    def enter_composite_transform(self, transform_node):
        print(f"enter_composite_transform({transform_node.full_label})")

    def leave_composite_transform(self, transform_node):
        print(f"leave_composite_transform({transform_node.full_label})")
