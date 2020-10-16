from __future__ import annotations

import asyncio
import ssl
import weakref
from typing import Awaitable, Callable, Dict, List, NamedTuple

from aiohttp import WSCloseCode, web
from aiojobs.aiohttp import setup

from .common import IncomingRequestHandler, Token, _contstruct_node_message
from .proto.gen.node_pb2 import Direction
from .websocket_base import WebsocketBase

_AioHttpWebsocketHandler = Callable[[web.Request], Awaitable[web.WebSocketResponse]]
WebsocketHandler = Callable[
    [web.Request, web.WebSocketResponse], Awaitable[web.WebSocketResponse]
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
                    return await route.handler(request, wsr)
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
        setup(app=self.app)

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

    async def serve(self) -> None:
        await self.start()
        names = sorted(str(s.name) for s in self.runner.sites)
        print(
            "======== Running on {} ========\n"
            "(Press CTRL+C to quit)".format(", ".join(names))
        )
        try:
            while True:
                await asyncio.sleep(3600)  # sleep forever in 1 hour intervals
        finally:
            await self.runner.cleanup()

    async def __aenter__(self) -> WebsocketServer:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> WebsocketServer:
        await self.runner.cleanup()
        return self


RESPONSE_TIMEOUT = 2

"""
Really all we need here is a handler that says for message in ws, recieve()
and a serverclient that is created before running that
"""


class ServerClient:
    def __init__(
        self,
        id: str,
        websocket: web.WebSocketResponse,
        incoming_request_handler: IncomingRequestHandler,
    ):
        self.id = id
        self.incoming_request_handler = incoming_request_handler

        self.request_dict = {}  # type: Dict[str, asyncio.Queue]
        self._receive_task = None

        self._base = WebsocketBase(
            websocket=websocket,
            incoming_direction=Direction.NodeToServer,
            incoming_request_handler=incoming_request_handler,
        )

    def initialize(self):
        self._base.initialize()

    async def receive_messages(self):
        await self._base.recieve_messages()

    async def request(self, data: bytes) -> bytes:
        return await self._base.request(
            _contstruct_node_message(data, Direction.ServerToNode)
        )
