import hamcrest as hc
import unittest

import ray

import apache_beam.portability.api.beam_fn_api_pb2
from ray_beam_runner.portability.execution import RayStateManager

class StateHandlerTest(unittest.TestCase):
  SAMPLE_STATE_KEY = apache_beam.portability.api.beam_fn_api_pb2.StateKey()
  SAMPLE_INPUT_DATA = [
      b'bobby'
      b'tables',
      b'drop table',
      b'where table_name > 12345'
  ]

  @classmethod
  def setUpClass(cls) -> None:
    if not ray.is_initialized():
      ray.init()

  @classmethod
  def tearDownClass(cls) -> None:
    ray.shutdown()

  def test_data_stored_properly(self):
    sh = RayStateManager()
    with sh.process_instruction_id('anyinstruction'):
      for data in StateHandlerTest.SAMPLE_INPUT_DATA:
        sh.append_raw(StateHandlerTest.SAMPLE_STATE_KEY, data)

    with sh.process_instruction_id('anyinstruction'):
      continuation_token = None
      all_data = []
      while True:
        data, continuation_token = sh.get_raw(StateHandlerTest.SAMPLE_STATE_KEY, continuation_token)
        all_data.append(data)
        if continuation_token is None:
          break

    hc.assert_that(all_data, hc.contains_exactly(*StateHandlerTest.SAMPLE_INPUT_DATA))
