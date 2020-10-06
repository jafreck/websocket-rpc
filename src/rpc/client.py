import asyncio
import ssl
import uuid
from typing import Awaitable, Callable

import aiohttp
from aiohttp import WSMsgType

import proto.gen.node_pb2
from server import Token

# constants
_AUTHORIZATION_HEADER_KEY = "Authorization"

IncomingMessageHandler = Callable[[bytes], Awaitable[bytes]]


class WebsocketClient:
    def __init__(
        self,
        connect_address: str,
        incoming_message_handler: IncomingMessageHandler,
        ssl_context: ssl.SSLContext = None,
        token: Token = None,
    ):
        self.connect_address = connect_address
        self.incoming_message_handler = incoming_message_handler
        self.ssl_context = ssl_context
        self.token = token

        self.session = aiohttp.ClientSession()
        self.request_dict = dict()  # {request_id: int : queue: asyncio.Queue}
        self._lock = asyncio.Lock()

        # instantiated outside of __init__
        self._ws = None
        self._receive_task = None
        self._reconnect_websocket_task = None

    async def close(self):
        try:
            self._receive_task.cancel()
        except Exception as ex:
            print(
                f"WebsocketClient.close encountered exception while cancelling receive_task: {ex}"
            )
            pass
        try:
            self._reconnect_websocket_task.cancel()
        except Exception as ex:
            print(
                f"WebsocketClient.close encountered exception while cancelling _reconnect_websocket_task: {ex}"
            )
            pass
        try:
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
        except Exception as ex:
            print(f"WebsocketClient.close encountered exception while closing ws: {ex}")
            pass

        try:
            await self.session.close()
        except Exception as ex:
            print(
                f"WebsocketClient.close encountered exception while closing session: {ex}"
            )
            pass

    async def connect(self):
        async with self._lock:
            await self._connect()

    async def __aenter__(self):
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        return self

    async def _connect(self):
        print(f"connecting to {self.connect_address}, ssl_context={self.ssl_context}")
        try:
            headers = {}
            if self.token is not None:
                headers = {_AUTHORIZATION_HEADER_KEY: self.token.value}
            self._ws = await self.session.ws_connect(
                self.connect_address,
                ssl=self.ssl_context,
                headers=headers,
            )
        except Exception as ex:
            print(f"Exception caught connecting to {self.connect_address}: {type(ex)}")
            raise ex

        loop = asyncio.get_event_loop()
        self._receive_task = loop.create_task(self._receive_from_websocket())
        self._reconnect_websocket_task = loop.create_task(self._websocket_monitor())

    async def _websocket_monitor(self):
        while True:
            print("_websocket_monitor running...")
            await self._reconnect_websocket_if_needed()
            await asyncio.sleep(1)
            print("_websocket_monitor done sleeping...")

    async def _reconnect_websocket_if_needed(self):
        print("_reconnect_websocket_if_needed trying to take lock")
        async with self._lock:
            print("_reconnect_websocket_if_needed took lock")
            if (
                self._ws is None
                or self._ws.closed
                or self._receive_task is None
                or self._receive_task.done()
            ):
                # TODO: cleanup existing requests?
                # may need to take a lock here
                print(
                    f"Reconnecting websocket: self._ws={self._ws}, "
                    + f"self._ws.closed={self._ws.closed if self._ws else None}, "
                    + f"self._receive_task.done={self._receive_task.done() if self._receive_task else None}"
                )

                if self._receive_task is not None:
                    self._receive_task.cancel()

                if self._reconnect_websocket_task is not None:
                    self._reconnect_websocket_task.cancel()

                await self._connect()

    async def _receive_from_websocket(self):
        try:
            while self._ws is not None or self._ws.closed or self._receive_task.done():
                ws_msg = await self._ws.receive()
                # print(f"client received websocket message: {ws_msg}")
                if ws_msg.type == WSMsgType.BINARY:
                    node_msg = proto.gen.node_pb2.NodeMessage()
                    node_msg.ParseFromString(ws_msg.data)
                    print(f"client received node_msg: {node_msg}")
                    if node_msg.direction == proto.gen.node_pb2.Direction.ServerToNode:
                        # TODO: handle incoming server request
                        await self.incoming_message_handler()
                    if node_msg.direction == proto.gen.node_pb2.Direction.NodeToServer:
                        response_queue = self.request_dict[node_msg.id]
                        response_queue.put_nowait(node_msg)
                elif ws_msg.type == aiohttp.WSMsgType.ERROR:
                    break
                elif ws_msg.type == aiohttp.WSMsgType.CLOSED:
                    break
                # TODO: handle all other message types
        except asyncio.CancelledError:
            print(f"{self._receive_from_websocket.__name__} cancelled")
            raise
        except Exception as e:
            print(f"{self._receive_from_websocket.__name__} encountered exception {e}")

    async def request(self, data: bytes) -> bytes:
        await self._reconnect_websocket_if_needed()

        node_msg = proto.gen.node_pb2.NodeMessage()
        node_msg.id = str(uuid.uuid4())
        node_msg.bytes = data
        node_msg.direction = proto.gen.node_pb2.Direction.NodeToServer
        await self._ws.send_bytes(node_msg.SerializeToString())

        response_queue = asyncio.Queue()
        self.request_dict[node_msg.id] = response_queue

        try:
            response = await asyncio.wait_for(response_queue.get(), 2)
            print(
                f"client received response: {response}, request_dict={self.request_dict.keys()}"
            )
            return response.bytes
        finally:
            del self.request_dict[node_msg.id]
