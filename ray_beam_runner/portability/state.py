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

"""Library for streaming state management for Ray Beam Runner."""

import collections
import contextlib
from typing import Optional, Tuple, Iterator, TypeVar

import ray
from ray import ObjectRef
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker import sdk_worker

T = TypeVar("T")


class RayFuture(sdk_worker._Future[T]):
    """Wraps a ray ObjectRef in a beam sdk_worker._Future"""

    def __init__(self, object_ref):
        # type: (ObjectRef[T]) -> None
        self._object_ref: ObjectRef[T] = object_ref

    def wait(self, timeout=None):
        # type: (Optional[float]) -> bool
        try:
            ray.get(self._object_ref, timeout=timeout)
            #
            return True
        except ray.GetTimeoutError:
            return False

    def get(self, timeout=None):
        # type: (Optional[float]) -> T
        return ray.get(self._object_ref, timeout=timeout)

    def set(self, _value):
        # type: (T) -> sdk_worker._Future[T]
        raise NotImplementedError()


@ray.remote
class _ActorStateManager:
    def __init__(self):
        self._data = collections.defaultdict(lambda: [])

    def get_raw(
        self,
        state_key: str,
        continuation_token: Optional[bytes] = None,
    ) -> Tuple[bytes, Optional[bytes]]:
        if continuation_token:
            continuation_token = int(continuation_token)
        else:
            continuation_token = 0

        full_state = self._data[state_key]
        if len(full_state) == continuation_token:
            return b"", None

        if continuation_token + 1 == len(full_state):
            next_cont_token = None
        else:
            next_cont_token = str(continuation_token + 1).encode("utf8")

        return full_state[continuation_token], next_cont_token

    def append_raw(self, state_key: str, data: bytes):
        self._data[state_key].append(data)

    def clear(self, state_key: str):
        self._data[state_key] = []


class RayStateManager(sdk_worker.StateHandler):
    def __init__(self, state_actor: Optional[_ActorStateManager] = None):
        self._state_actor = state_actor or _ActorStateManager.remote()
        self._instruction_id = None

    @staticmethod
    def _to_key(state_key: beam_fn_api_pb2.StateKey):
        return state_key.SerializeToString()

    def get_raw(
        self,
        state_key,  # type: beam_fn_api_pb2.StateKey
        continuation_token=None,  # type: Optional[bytes]
    ) -> Tuple[bytes, Optional[bytes]]:
        assert self._instruction_id is not None
        return ray.get(
            self._state_actor.get_raw.remote(
                RayStateManager._to_key(state_key),
                continuation_token,
            )
        )

    def append_raw(self, state_key: beam_fn_api_pb2.StateKey, data: bytes) -> RayFuture:
        assert self._instruction_id is not None
        return RayFuture(
            self._state_actor.append_raw.remote(
                RayStateManager._to_key(state_key), data
            )
        )

    def clear(self, state_key: beam_fn_api_pb2.StateKey) -> RayFuture:
        assert self._instruction_id is not None
        return RayFuture(
            self._state_actor.clear.remote(RayStateManager._to_key(state_key))
        )

    @contextlib.contextmanager
    def process_instruction_id(self, bundle_id: str) -> Iterator[None]:
        # Instruction id is not being used right now,
        # we only assert that it has been set before accessing state.
        self._instruction_id = bundle_id
        yield
        self._instruction_id = None

    def done(self):
        pass
