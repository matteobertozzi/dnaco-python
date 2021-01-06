import asyncio

from dnaco.rpc.frame import build_frame_header
from dnaco.rpc.server import read_frame, frame_handle


async def client_loop(port):
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    try:
        writer.write(build_frame_header(7, False, 10))
        writer.write(b'x' * 10)
        await writer.drain()

        rev, write_to_wal, data = await read_frame(reader)
        print('CLIENT: received packet', rev, write_to_wal, data)

    finally:
        writer.close()
        await writer.wait_closed()


async def echo_handler(rev, write_to_wal, data):
    print('ECHO SERVER: received packet', rev, write_to_wal, data)
    return rev, write_to_wal, data


async def server_loop(port):
    server = await asyncio.start_server(lambda r, w: frame_handle(r, w, echo_handler), '127.0.0.1', port)

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
