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

"""Set of utilities for execution of a pipeline by the FnApiRunner."""

# mypy: disallow-untyped-defs

import contextlib
import collections
from typing import Iterator
from typing import Optional
from typing import Tuple

import ray

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker import sdk_worker


@ray.remote
class _ActorStateManager:
  def __init__(self):
    self._data = collections.defaultdict(lambda : [])

  def get_raw(
      self,
      bundle_id: str,
      state_key: str,
      continuation_token: Optional[bytes] = None,
  ) -> Tuple[bytes, Optional[bytes]]:
    if continuation_token:
      continuation_token = int(continuation_token)
    else:
      continuation_token = 0

    new_cont_token = continuation_token + 1
    if len(self._data[(bundle_id, state_key)]) == new_cont_token:
      return self._data[(bundle_id, state_key)][continuation_token], None
    else:
      return (self._data[(bundle_id, state_key)][continuation_token],
              str(continuation_token + 1).encode('utf8'))

  def append_raw(
      self,
      bundle_id: str,
      state_key: str,
      data: bytes
  ):
    self._data[(bundle_id, state_key)].append(data)

  def clear(self, bundle_id: str, state_key: str):
    self._data[(bundle_id, state_key)] = []


class RayStateManager(sdk_worker.StateHandler):
  def __init__(self, state_actor: Optional[_ActorStateManager] = None):
    self._state_actor = state_actor or _ActorStateManager.remote()
    self._instruction_id: Optional[str] = None

  @staticmethod
  def _to_key(state_key: beam_fn_api_pb2.StateKey):
    return state_key.SerializeToString()

  def get_raw(
      self,
      state_key,  # type: beam_fn_api_pb2.StateKey
      continuation_token=None  # type: Optional[bytes]
  ) -> Tuple[bytes, Optional[bytes]]:
    assert self._instruction_id is not None
    return ray.get(
        self._state_actor.get_raw.remote(self._instruction_id, RayStateManager._to_key(state_key), continuation_token))

  def append_raw(
      self,
      state_key: beam_fn_api_pb2.StateKey,
      data: bytes
  ) -> sdk_worker._Future:
    assert self._instruction_id is not None
    return self._state_actor.append_raw.remote(self._instruction_id, RayStateManager._to_key(state_key), data)

  def clear(self, state_key: beam_fn_api_pb2.StateKey) -> sdk_worker._Future:
    # TODO(pabloem): Does the ray future work as a replacement of Beam _Future?
    assert self._instruction_id is not None
    return self._state_actor.clear.remote(self._instruction_id, RayStateManager._to_key(state_key))

  @contextlib.contextmanager
  def process_instruction_id(self, bundle_id: str) -> Iterator[None]:
    self._instruction_id = bundle_id
    yield
    self._instruction_id = None

  def done(self):
    pass
