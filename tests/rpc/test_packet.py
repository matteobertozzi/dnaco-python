from unittest import TestCase

from dnaco.rpc.frame import FrameReader, parse_frame_header
from dnaco.rpc.packet import RpcPacketBuilder, RpcRequest, RpcResponse, parse_rpc_packet


class TestRpcPacket(TestCase):
    def test_simple_req_resp(self):
        pkg_builder = RpcPacketBuilder(1)

        req = pkg_builder.new_request(1, RpcRequest.OP_TYPE_READ, b'/foo', b'req-body')
        self.assertEqual(req, b'\x10\x00\x00\x11\x00\x01\x01\x00\x03/fooreq-body')

        req_reader = FrameReader(req)
        rev, write_to_wal, length = parse_frame_header(req_reader.read(4))
        req_packet = parse_rpc_packet(req_reader)
        self.assertEqual(rev, 1)
        self.assertEqual(write_to_wal, False)
        self.assertEqual(length, 17)
        self.assertEqual(req_packet.op_type, RpcRequest.OP_TYPE_READ)
        self.assertEqual(req_packet.request_id, b'/foo')
        self.assertEqual(req_packet.body, b'req-body')

        resp = pkg_builder.new_response(req_packet.trace_id, req_packet.pkg_id, RpcResponse.OP_STATUS_SUCCEEDED, 123, 456, b'resp-body')
        resp_reader = FrameReader(resp)
        rev, write_to_wal, length = parse_frame_header(resp_reader.read(4))
        resp_packet = parse_rpc_packet(resp_reader)
        self.assertEqual(rev, 1)
        self.assertEqual(write_to_wal, False)
        self.assertEqual(length, 16)
        self.assertEqual(resp_packet.trace_id, req_packet.trace_id)
        self.assertEqual(resp_packet.pkg_id, req_packet.pkg_id)
        self.assertEqual(resp_packet.op_status, RpcResponse.OP_STATUS_SUCCEEDED)
        self.assertEqual(resp_packet.queue_time, 123)
        self.assertEqual(resp_packet.exec_time, 456)
        self.assertEqual(resp_packet.body, b'resp-body')
