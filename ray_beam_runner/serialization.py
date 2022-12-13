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

from apache_beam.portability.api import beam_runner_api_pb2, beam_fn_api_pb2


def register_protobuf_serializers():
    """
    Register serializers for protobuf messages.
    Note: Serializers are managed locally for each Ray worker.
    """
    # TODO(rkenmi): Figure out how to not repeat this call on workers?
    pb_msg_map = {
        msg_name: pb_module
        for pb_module in [beam_fn_api_pb2, beam_runner_api_pb2]
        for msg_name in pb_module.DESCRIPTOR.message_types_by_name.keys()
    }

    def _serializer(message):
        return message.SerializeToString()

    def _deserializer(pb_module, msg_name):
        return lambda s: getattr(pb_module, msg_name).FromString(s)

    for msg_name, pb_module in pb_msg_map.items():
        ray.util.register_serializer(
            getattr(pb_module, msg_name),
            serializer=_serializer,
            deserializer=_deserializer(pb_module, msg_name),
        )
