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

import asyncio

class Mailbox:
    def __init__(self, client):
        self.out_queue = asyncio.Queue()
        self.in_queue = asyncio.Queue()
        self.client = client
        self.running = False
        self._task_send = None
        self._task_recv = None

    async def start(self):
        self._task_send = asyncio.create_task(self._send_loop())
        self._task_recv = asyncio.create_task(self._recv_loop())
        self.running = True

    async def stop(self):
        self.running = False
        await self.out_queue.put(None)
        await self.in_queue.put(None)

    async def send(self, req):
        await self.out_queue.put(req)

    async def recv(self):
        return await self.in_queue.get()

    async def _send_loop(self):
        while True:
            packet = await self.out_queue.get()
            try:
                if packet is None:
                    break
                # send data to the server
                await self.client.send_packet(packet)
            finally:
                self.out_queue.task_done()

    async def _recv_loop(self):
        while True:
            packet = await self.client.fetch_packet()
            if packet is None:
                break
            # send data to the server
            await self.in_queue.put(packet)


class InMemoryClient:
    def __init__(self):
        self.server_out_queue = asyncio.Queue()
        self.server_in_queue = asyncio.Queue()

    async def send_packet(self, packet):
        return await self.server_in_queue.put(packet)

    async def fetch_packet(self):
        try:
            return await self.server_out_queue.get()
        finally:
            self.server_out_queue.task_done()


class InMemoryServer:
    def __init__(self, mailbox):
        self.running = True
        self.mailbox = mailbox

    async def stop(self):
        self.running = False
        await self.mailbox.server_in_queue.put(None)
        await self.mailbox.server_out_queue.put(None)

    async def loop(self):
        while self.running:
            data = await self.mailbox.server_in_queue.get()
            if data is None:
                break
            await self.mailbox.server_out_queue.put('reply-' + data)


async def main():
    client = InMemoryClient()
    mailbox = Mailbox(client)
    await mailbox.start()

    server = InMemoryServer(client)
    server_task = asyncio.create_task(server.loop())

    for i in range(4):
        await mailbox.send('hello-%d' % i)
        recv_data = await mailbox.recv()
        print('recv:', recv_data)

    await server.stop()
    await mailbox.stop()
    await server_task
    print('nothing more')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
