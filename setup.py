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
from setuptools import find_packages, setup

TEST_REQUIREMENTS = [
    'apache_beam[test]',
    'pyhamcrest',
    'pytest',
]

setup(
    name="ray_beam",
    packages=find_packages(where=".", include="ray_beam_runner*"),
    version="0.0.1",
    author="Ray Team",
    description="An Apache Beam Runner using Ray.",
    long_description="An Apache Beam Runner based on the Ray "
    "distributed computing framework.",
    url="https://github.com/ray-project/ray_beam_runner",
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    install_requires=[
        "ray[data]", "apache_beam"
    ],
    extras_require={
        'test': TEST_REQUIREMENTS,
    }
)
