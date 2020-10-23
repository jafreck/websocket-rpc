from __future__ import annotations

import asyncio
import logging
import ssl
import traceback
import weakref
from typing import Awaitable, Callable, Dict, List, NamedTuple

import aiojobs
from aiohttp import WSCloseCode, web

from . import log
from .common import IncomingRequestHandler, Token
from .proto.gen.node_pb2 import (
    MessageDirection,
    NodeMessage,
    NodeMessageCompleteRequest,
)
from .websocket_base import WebsocketBase

_AioHttpWebsocketHandler = Callable[[web.Request], Awaitable[web.WebSocketResponse]]
WebsocketHandler = Callable[
    ["WebsocketServer", web.Request, web.WebSocketResponse],
    Awaitable[web.WebSocketResponse],
]
PrePrepareHook = Callable[["WebsocketServer", web.Request], Awaitable[None]]


class Route(NamedTuple):
    path: str
    handler: WebsocketHandler
    pre_prepare_hook: PrePrepareHook = None


class WebsocketServer:
    def __init__(
        self,
        host: str,
        port: int,
        routes: List[Route],
        ssl_context: ssl.SSLContext = None,
        tokens: Dict[str, Token] = None,
    ):
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.tokens = tokens or {}  # type: Dict[str, Token]

        self.routes = routes
        self.app = web.Application()
        self.app["websockets"] = weakref.WeakSet()  # store open connections
        self.app.on_shutdown.append(self.on_shutdown)
        self.runner = web.AppRunner(self.app)
        self.started = False

        self.log = log

    def _generate_handler(
        self: WebsocketServer, route: Route
    ) -> _AioHttpWebsocketHandler:
        async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
            if route.pre_prepare_hook is not None:
                await route.pre_prepare_hook(self, request)
            wsr = web.WebSocketResponse()
            await wsr.prepare(request)
            request.app["websockets"].add(wsr)
            try:
                if route.handler is not None:
                    return await route.handler(self, request, wsr)
            except Exception as e:
                log.info(
                    f"handler exception={type(e)}, {e}, {traceback.extract_tb(e.__traceback__)}"
                )
                raise
            finally:
                request.app["websockets"].discard(wsr)

            return wsr

        return websocket_handler

    async def on_shutdown(self, app: web.Application) -> None:
        for ws in set(self.app["websockets"]):
            await ws.close(code=WSCloseCode.GOING_AWAY, message="Server shutdown")

    async def start(self) -> None:
        if self.started:
            raise Exception("already started")

        # start aiojobs scheduler
        aiojobs.aiohttp.setup(app=self.app)

        self.started = True

        for route in self.routes:
            self.app.add_routes([web.get(route.path, self._generate_handler(route))])

        await self.runner.setup()
        site = web.TCPSite(
            self.runner,
            host=self.host,
            port=self.port,
            ssl_context=self.ssl_context,
        )
        await site.start()

    async def __aenter__(self) -> WebsocketServer:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> WebsocketServer:
        log.info(
            f"__aexit__: exc_type={exc_type}, exc={exc}, tb={traceback.extract_tb(tb)}"
        )
        await self.runner.cleanup()
        if exc is not None:
            raise exc
        return self


RESPONSE_TIMEOUT = 2  # TODO: make this configurable and part of the protocol


class ServerClient:
    def __init__(
        self,
        id: str,
        websocket: web.WebSocketResponse,
        incoming_request_handler: IncomingRequestHandler,
        scheduler: aiojobs.Scheduler,
    ):
        self.id = id
        self.incoming_request_handler = incoming_request_handler

        self.request_dict = {}  # type: Dict[str, asyncio.Queue]
        self._receive_task = None

        self._base = WebsocketBase(
            websocket=websocket,
            incoming_direction=MessageDirection.NodeToServer,
            incoming_request_handler=incoming_request_handler,
            scheduler=scheduler,
        )
        self.log = log

    async def initialize(self):
        await self._base.initialize()

    async def receive_messages(self):
        await self._base.recieve_messages()

    async def request(self, data: bytes) -> bytes:
        log.info(f"server client sending request: {data}")
        node_msg = NodeMessage()
        node_msg.fullRequest.CopyFrom(NodeMessageCompleteRequest(bytes=data))
        node_msg.direction = MessageDirection.ServerToNode

        return await self._base.request(node_msg)
