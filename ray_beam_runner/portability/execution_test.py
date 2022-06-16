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

import hamcrest as hc
import unittest

import ray

from apache_beam.portability.api import beam_fn_api_pb2
from ray_beam_runner.portability.state import RayStateManager


class StateHandlerTest(unittest.TestCase):
    SAMPLE_STATE_KEY = beam_fn_api_pb2.StateKey()
    SAMPLE_INPUT_DATA = [b"bobby" b"tables", b"drop table", b"where table_name > 12345"]

    @classmethod
    def setUpClass(cls) -> None:
        if not ray.is_initialized():
            ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_data_stored_properly(self):
        sh = RayStateManager()
        with sh.process_instruction_id("anyinstruction"):
            for data in StateHandlerTest.SAMPLE_INPUT_DATA:
                sh.append_raw(StateHandlerTest.SAMPLE_STATE_KEY, data)

        with sh.process_instruction_id("anyinstruction"):
            continuation_token = None
            all_data = []
            while True:
                data, continuation_token = sh.get_raw(
                    StateHandlerTest.SAMPLE_STATE_KEY, continuation_token
                )
                all_data.append(data)
                if continuation_token is None:
                    break

        hc.assert_that(
            all_data, hc.contains_exactly(*StateHandlerTest.SAMPLE_INPUT_DATA)
        )
