from dnaco.telemetry.collector import TelemetryCollector
from dnaco.telemetry.time_range_counter import TimeRangeCounter
from dnaco.util import humans
from .frame import parse_frame_header, build_frame_header

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
    rev, write_to_wal, length = parse_frame_header(header)
    frame_data = await reader.readexactly(length)
    server_rx_bytes.add(4 + length)
    server_rx_frames.inc()
    return rev, write_to_wal, frame_data


async def frame_handle(reader, writer, handler):
    try:
        while not reader.at_eof():
            # wait for a frame...
            rev, write_to_wal, data = await read_frame(reader)
            # let the handler compute a new frame
            rev, write_to_wal, data = await handler(rev, write_to_wal, data)
            # write back the frame as response
            writer.write(build_frame_header(rev, write_to_wal, len(data)))
            writer.write(data)
            await writer.drain()
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
