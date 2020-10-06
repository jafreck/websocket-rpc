import asyncio
import ssl
import uuid
from typing import Awaitable, Callable, List, NamedTuple

from aiohttp import web

from common import Token

_AioHttpWebsocketHandler = Callable[[web.Request], Awaitable[web.WebSocketResponse]]
WebsocketHandler = Callable[[web.WebSocketResponse], Awaitable[web.WebSocketResponse]]
PrePrepareHook = Callable[["WebsocketServer", web.Request], Awaitable[None]]


class Route(NamedTuple):
    path: str
    handler: WebsocketHandler
    pre_prepare_hook: PrePrepareHook = None


def generate_token() -> Token:
    return Token(value=str(uuid.uuid4()))


class WebsocketServer:
    def __init__(
        self,
        host: str,
        port: int,
        ssl_context: ssl.SSLContext = None,
    ):
        self.host = host
        self.port = port
        self.ssl_context = ssl_context  # TODO: impl ssl
        self.app = web.Application()
        self.runner = web.AppRunner(self.app)

        self.started = False
        self.tokens = []  # type: List[Token]

    def _generate_handler(
        self: "WebsocketServer", route: Route
    ) -> _AioHttpWebsocketHandler:
        async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
            if route.pre_prepare_hook is not None:
                await route.pre_prepare_hook(self, request)
            wsr = web.WebSocketResponse()
            await wsr.prepare(request)
            if route.handler is not None:
                return await route.handler(wsr)
            return wsr

        return websocket_handler

    async def start(
        self: "WebsocketServer", routes: List[Route], num_tokens=1
    ) -> List[Token]:
        if self.started:
            raise Exception("already started")

        self.started = True
        for _ in range(num_tokens):
            self.tokens.append(generate_token())

        for route in routes:
            self.app.add_routes([web.get(route.path, self._generate_handler(route))])

        await self.runner.setup()
        site = web.TCPSite(
            self.runner,
            host=self.host,
            port=self.port,
            ssl_context=self.ssl_context,
        )
        await site.start()

        return self.tokens

    async def serve(self, routes: List[Route]):
        await self.start(routes)
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
