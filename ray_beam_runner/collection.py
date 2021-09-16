import ray

from apache_beam.pvalue import PValue
from apache_beam.typehints import Dict


class CollectionMap:
    def __init__(self):
        self.ray_datasets: Dict[PValue, ray.data.Dataset] = {}

    def get(self, pvalue: PValue):
        return self.ray_datasets.get(pvalue, None)

    def set(self, pvalue: PValue, ray_dataset: ray.data.Dataset):
        self.ray_datasets[pvalue] = ray_dataset

    def has(self, pvalue: PValue):
        return pvalue in self.ray_datasets
