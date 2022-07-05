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
#
FROM python:3.9.13-bullseye

RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-setuptools vim less

RUN pip install --upgrade pip

COPY dist/*.whl root/
RUN pip install root/*.whl

COPY requirements_dev.txt root/requirements.txt
RUN pip install -r root/requirements.txt
