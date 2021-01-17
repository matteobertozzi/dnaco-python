# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import TestCase

from dnaco.rpc.frame import FrameReader, parse_frame_header
from dnaco.rpc.packet import RpcPacketBuilder, RpcRequest, RpcResponse, parse_rpc_packet


class TestRpcPacket(TestCase):
    def test_simple_req_resp(self):
        pkg_builder = RpcPacketBuilder(1)

        req = pkg_builder.new_request(1, RpcRequest.OP_TYPE_READ, b'/foo', b'req-body')
        self.assertEqual(req, b'\x00\x01\x01\x00\x03/fooreq-body')

        req_reader = FrameReader(req)
        req_packet = parse_rpc_packet(req_reader)
        self.assertEqual(len(req), 17)
        self.assertEqual(req_packet.op_type, RpcRequest.OP_TYPE_READ)
        self.assertEqual(req_packet.request_id, b'/foo')
        self.assertEqual(req_packet.body, b'req-body')

        resp = pkg_builder.new_response(req_packet.trace_id, req_packet.pkg_id, RpcResponse.OP_STATUS_SUCCEEDED, 123, 456, b'resp-body')
        resp_reader = FrameReader(resp)
        resp_packet = parse_rpc_packet(resp_reader)
        self.assertEqual(len(resp), 16)
        self.assertEqual(resp_packet.trace_id, req_packet.trace_id)
        self.assertEqual(resp_packet.pkg_id, req_packet.pkg_id)
        self.assertEqual(resp_packet.op_status, RpcResponse.OP_STATUS_SUCCEEDED)
        self.assertEqual(resp_packet.queue_time, 123)
        self.assertEqual(resp_packet.exec_time, 456)
        self.assertEqual(resp_packet.body, b'resp-body')
