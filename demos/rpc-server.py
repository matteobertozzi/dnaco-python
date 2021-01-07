import asyncio

from dnaco.rpc.frame import FrameReader, build_frame_header
from dnaco.rpc.packet import RpcPacketBuilder, RpcRequest, parse_rpc_packet
from dnaco.rpc.server import read_frame, rpc_handle, rpc_handler


async def client_loop(port):
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    try:
        pkg_builder = RpcPacketBuilder(1)
        req = pkg_builder.new_request(1, RpcRequest.OP_TYPE_READ, b'/test', b'req-body')
        print('write', req)
        writer.write(build_frame_header(1, False, len(req)))
        writer.write(req)
        await writer.drain()

        _, rev, write_to_wal, data = await read_frame(reader)
        resp_reader = FrameReader(data)
        resp_packet = parse_rpc_packet(resp_reader)
        print('CLIENT: received packet', rev, write_to_wal, resp_packet)

    finally:
        writer.close()
        await writer.wait_closed()


@rpc_handler('/test')
def test_handle(packet):
    print('test handle', packet)
    return b'resp-test-body'


async def server_loop(port):
    server = await asyncio.start_server(rpc_handle, '127.0.0.1', port)

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    return server


async def main():
    port = 57025
    server = await server_loop(port)
    task_client = asyncio.create_task(client_loop(port))

    await task_client
    async with server:
        await server.wait_closed()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
