import asyncio
import ssl
import traceback
import uuid
from typing import Dict

import aiohttp
import aiojobs

from .common import IncomingRequestHandler, Token
from .proto.gen.node_pb2 import (
    MessageDirection,
    NodeMessage,
    NodeMessageCompleteRequest,
)
from .websocket_base import WebsocketBase

# constants
_AUTHENTICATION_HEADER_KEY = "Authentication"
_NODE_ID_HEADER_KEY = "x-ms-nodeidentifier"


class WebsocketClient:
    def __init__(
        self,
        connect_address: str,
        incoming_request_handler: IncomingRequestHandler,
        ssl_context: ssl.SSLContext = None,
        token: Token = None,
        id: str = None,
    ):
        self.connect_address = connect_address
        self._incoming_request_handler = incoming_request_handler
        self.ssl_context = ssl_context
        self.token = token
        self.id = id if id is not None else str(uuid.uuid4())

        self.session = aiohttp.ClientSession()
        self.request_dict = {}  # type: Dict[str, asyncio.Queue]
        self._lock = asyncio.Lock()

        # instantiated outside of __init__
        self._ws = None  # type: web.WebsocketResponse
        self._reconnect_websocket_task = None  # type: asyncio.Task

        # TODO: replace all create_tasks and run_until_completes with _scheduler.spawn
        self._scheduler = None  # type: aiojobs.Scheduler

        self._base = None  # type: WebsocketBase

    async def close(self) -> None:
        print("client closing...")
        try:
            await self._reconnect_websocket_task.close()
        except Exception as ex:
            print(
                f"WebsocketClient.close encountered exception while cancelling _reconnect_websocket_task: {ex}"
            )
        try:
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
        except Exception as ex:
            print(f"WebsocketClient.close encountered exception while closing ws: {ex}")

        try:
            await self.session.close()
        except Exception as ex:
            print(
                f"WebsocketClient.close encountered exception while closing session: {ex}"
            )

    async def connect(self):
        async with self._lock:
            await self._connect()

    async def __aenter__(self):
        self._scheduler = await aiojobs.create_scheduler()
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        print(
            f"__aexit__: exc_type={exc_type}, exc={exc}, tb={traceback.extract_tb(tb)}"
        )

        if exc is not None:
            raise exc
        await self.close()
        await self._scheduler.close()
        return self

    async def _connect(self) -> None:
        print(f"connecting to {self.connect_address}, ssl_context={self.ssl_context}")
        try:
            headers = {}
            if self.token is not None:
                headers = {
                    _AUTHENTICATION_HEADER_KEY: self.token.value,
                    _NODE_ID_HEADER_KEY: self.id,
                }
            self._ws = await self.session.ws_connect(
                self.connect_address,
                ssl=self.ssl_context,
                headers=headers,
            )
        except Exception as ex:
            print(f"Exception caught connecting to {self.connect_address}: {type(ex)}")
            raise ex

        if self._scheduler is None:
            # TODO: make this an init method?
            self._scheduler = await aiojobs.create_scheduler()

        self._base = WebsocketBase(
            websocket=self._ws,
            incoming_direction=MessageDirection.ServerToNode,
            incoming_request_handler=self._incoming_request_handler,
            scheduler=self._scheduler,
        )
        await self._base.initialize()

        self._reconnect_websocket_task = await self._scheduler.spawn(
            self._websocket_monitor()
        )

    async def _websocket_monitor(self) -> None:
        while True:
            try:
                await self._reconnect_websocket_if_needed()
            except Exception as e:
                print(f"reconnect failed with exception: {e}")
            await asyncio.sleep(1)

    async def _reconnect_websocket_if_needed(self) -> None:
        async with self._lock:
            if self._ws is None or self._ws.closed:
                # TODO: cleanup existing requests?
                # may need to take a lock here
                print(
                    f"Reconnecting websocket: self._ws={self._ws}, "
                    + f"self._ws.closed={self._ws.closed if self._ws else None}, "
                )

                if self._reconnect_websocket_task is not None:
                    await self._reconnect_websocket_task.close()

                await self._connect()

    async def request(self, data: bytes) -> bytes:
        await self._reconnect_websocket_if_needed()
        print(f"client sending request: {data}")

        node_msg = NodeMessage(id=str(uuid.uuid4()))
        node_msg.fullRequest.CopyFrom(NodeMessageCompleteRequest(bytes=data))
        node_msg.direction = MessageDirection.NodeToServer

        print(f"node_msg={node_msg}")
        return await self._base.request(node_msg)

    async def receive_messages(self) -> None:
        """
        block on receiving messages over the websocket
        """
        # TOOD: impl
        pass
