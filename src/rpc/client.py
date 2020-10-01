import asyncio
import queue
import ssl
import sys
import threading
import uuid
from enum import Enum
from typing import Optional

import aiohttp
from aiohttp import WSMsgType, request, web

import proto.gen.node_pb2


class WebsocketClient:
    def __init__(self, connect_address: str, ssl_context: ssl.SSLContext = None):
        self.connect_address = connect_address
        self.ssl_context = ssl_context
        self.session = aiohttp.ClientSession()
        self.request_dict = dict()  # {request_id: int : queue: asyncio.Queue}

        self.ws = None
        self.receive_task = None

    async def close(self):
        # TODO: create context manager instead?
        try:
            if self.ws is not None and not self.ws.closed:
                await self.ws.close()
        except:
            pass

        try:
            await self.session.close()
        except:
            pass

    async def connect(self):
        print(f"connecting to {self.connect_address}, ssl_context={self.ssl_context}")
        try:
            self.ws = await self.session.ws_connect(
                self.connect_address, ssl=self.ssl_context
            )
        except Exception as ex:
            print(f"Exception caught connecting to {self.connect_address}")
            raise ex

        self.receive_task = asyncio.get_event_loop().create_task(
            self._receive_from_websocket()
        )

    async def _receive_from_websocket(self):
        while True:
            response = await self.ws.receive()
            print(f"client received response: {response}")
            if response.type == WSMsgType.BINARY:
                node_msg = proto.gen.node_pb2.NodeMessage()
                node_msg.ParseFromString(response.data)
                print(f"client received node_msg: {node_msg}")

                response_queue = self.request_dict[node_msg.id]
                response_queue.put_nowait(node_msg)
            elif response.type == aiohttp.WSMsgType.ERROR:
                break
            elif response.type == aiohttp.WSMsgType.CLOSED:
                break
            # TODO: handle all other message types

    async def send_and_receive(self, data: bytes) -> bytes:
        if self.ws.closed:
            raise ConnectionError("websocket is closed")

        node_msg = proto.gen.node_pb2.NodeMessage()
        node_msg.id = str(uuid.uuid4())
        node_msg.bytes = data
        node_msg.direction = proto.gen.node_pb2.Direction.NodeToServer
        await self.ws.send_bytes(node_msg.SerializeToString())

        response_queue = asyncio.Queue()
        self.request_dict[node_msg.id] = response_queue
        response = await asyncio.wait_for(response_queue.get(), 1)

        print(f"client received response: {response}")

        self.request_dict.pop(response.id)

        return response.bytes
