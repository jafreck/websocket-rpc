import asyncio
import ssl
import sys
from enum import Enum
from typing import Optional

import aiohttp
from aiohttp import WSMsgType, request, web

import proto.gen.node_pb2


class WebsocketClient:
    def __init__(self, connect_address: str):
        self.connect_address = connect_address
        self.session = aiohttp.ClientSession()
        self.ws = None

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
        print(f"connecting to {self.connect_address}")
        try:
            self.ws = await self.session.ws_connect(self.connect_address)
        except Exception as ex:
            raise ConnectionError(
                f"Exception caught connecting to {self.connect_address}: {ex}"
            )

    async def send_str(self, msg: str):
        # TODO: check connection status
        if self.ws.closed:
            raise ConnectionError("websocket is closed")

        await self.ws.send_str(msg)

    async def send_msg(self, msg: proto.gen.node_pb2.NodeMessage):
        # TODO: check connection status
        if self.ws.closed:
            raise ConnectionError("websocket is closed")

        await self.ws.send_bytes(msg)

    async def recieve_msg(self) -> aiohttp.WSMessage:
        if self.ws.closed:
            raise ConnectionError("websocket is closed")

        return await self.ws.receive()

    async def receive_str(self) -> str:
        if self.ws.closed:
            raise ConnectionError("websocket is closed")

        return await self.ws.receive_str()

    async def recieve_node_msg(self) -> proto.gen.node_pb2.NodeMessage:
        if self.ws.closed:
            raise ConnectionError("websocket is closed")

        serialized_node_msg = await self.ws.receive_bytes()
        node_msg = proto.gen.node_pb2.NodeMessage()
        return node_msg.ParseFromString(serialized_node_msg)
