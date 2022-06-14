#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
