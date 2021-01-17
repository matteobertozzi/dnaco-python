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

from time import time_ns

from dnaco.rpc.packet import RpcRequest, RpcResponse
from dnaco.telemetry.collector import TelemetryCollector
from dnaco.telemetry.time_range_counter import TimeRangeCounter
from dnaco.telemetry.max_and_avg_time_range_counter import MaxAndAvgTimeRangeGauge
from dnaco.util import humans

from .frame import FrameReader, parse_frame_header, build_frame_header
from .packet import RpcPacketBuilder, parse_rpc_packet

# ==========================================================================================
#  AsyncIO Frame Handling
# ==========================================================================================
server_rx_bytes = TelemetryCollector.register(
    name='dnaco_rpc_rx_bytes',
    label='Bytes Read from the DNACO RPC Server',
    help_descr='Bytes Read divided by minute',
    unit=humans.HUMAN_SIZE,
    collector=TimeRangeCounter(60 * humans.UNIT_MIN, 1 * humans.UNIT_MIN)
)

server_rx_frames = TelemetryCollector.register(
    name='dnaco_rpc_rx_frames',
    label='Frames Read from the DNACO RPC Server',
    help_descr='Frames Read divided by minute',
    unit=humans.HUMAN_COUNT,
    collector=TimeRangeCounter(60 * humans.UNIT_MIN, 1 * humans.UNIT_MIN)
)

server_tx_bytes = TelemetryCollector.register(
    name='dnaco_rpc_tx_bytes',
    label='Bytes Written by the DNACO RPC Server',
    help_descr='Bytes Written divided by minute',
    unit=humans.HUMAN_SIZE,
    collector=TimeRangeCounter(60 * humans.UNIT_MIN, 1 * humans.UNIT_MIN)
)

server_tx_frames = TelemetryCollector.register(
    name='dnaco_rpc_tx_frames',
    label='Frames Written by the DNACO RPC Server',
    help_descr='Frames Written divided by minute',
    unit=humans.HUMAN_COUNT,
    collector=TimeRangeCounter(60 * humans.UNIT_MIN, 1 * humans.UNIT_MIN)
)


async def read_frame(reader):
    header = await reader.readexactly(4)
    start_ns = time_ns()
    rev, write_to_wal, length = parse_frame_header(header)
    frame_data = await reader.readexactly(length)
    server_rx_bytes.add(4 + length)
    server_rx_frames.inc()
    return start_ns, rev, write_to_wal, frame_data


async def frame_handle(reader, writer, handler):
    try:
        while not reader.at_eof():
            # wait for a frame...
            req_recv_ns, rev, write_to_wal, data = await read_frame(reader)

            # let the handler compute a new frame
            rev, write_to_wal, data = await handler(rev, write_to_wal, req_recv_ns, data)

            # write back the frame as response
            # print('write response', rev, write_to_wal, len(data), data)
            writer.write(build_frame_header(rev, write_to_wal, len(data)))
            writer.write(data)
            await writer.drain()

            # update packet stats
            server_tx_bytes.add(4 + len(data))
            server_tx_frames.inc()

    except Exception as e:
        # TODO: use logger
        if reader.at_eof():
            print('CLIENT closed')
        else:
            print('FAIL incomplete read', e)
    finally:
        writer.close()
        await writer.wait_closed()


# ==========================================================================================
#  AsyncIO RPC Handling
# ==========================================================================================
server_rpc_exec_time = TelemetryCollector.register(
    name='dnaco_rpc_exec_time',
    label='RPC Exec Time',
    help_descr='RPC Exec Time',
    unit=humans.HUMAN_TIME_NS,
    collector=MaxAndAvgTimeRangeGauge(60 * humans.UNIT_MIN, 1 * humans.UNIT_MIN)
)

_rpc_handlers = {}


def rpc_handler(name):
    def _handler(func):
        _rpc_handlers[name.encode('utf-8')] = func
        print('rpc handler', name, func)
        return func

    return _handler


async def packet_handle(rev, write_to_wal, req_recv_ns, data):
    reader = FrameReader(data)
    packet = parse_rpc_packet(reader)
    packet.rev = rev
    packet.write_to_wal = write_to_wal
    if isinstance(packet, RpcRequest):
        func = _rpc_handlers.get(packet.request_id)
        if not func:
            # TODO: handle with RPC NOT FOUND
            raise NotImplementedError

        start_ns = time_ns()
        queue_ns = start_ns - req_recv_ns
        resp_body = func(packet)
        exec_ns = time_ns() - start_ns

        pkg_builder = RpcPacketBuilder(rev)
        resp = pkg_builder.new_response(packet.trace_id, packet.pkg_id, RpcResponse.OP_STATUS_SUCCEEDED, queue_ns, exec_ns, resp_body)
        return rev, False, resp

    elif isinstance(packet, RpcResponse):
        # TODO: handle responses
        pass
    else:
        # TODO: handle other packets...
        raise NotImplementedError


async def rpc_handle(reader, writer):
    await frame_handle(reader, writer, packet_handle)
