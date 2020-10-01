import argparse
import asyncio
import ssl
import sys
from enum import Enum
from typing import Callable, List, NamedTuple, Optional

import aiohttp
from aiohttp import WSMsgType, request, web

import proto.gen.node_pb2


class Route(NamedTuple):
    path: str
    handler: Callable[[web.Request], web.WebSocketResponse]


class WebsocketServer:
    def __init__(self, host: str, port: int, ssl_context: ssl.SSLContext = None):
        self.host = host
        self.port = port
        self.ssl_context = ssl_context  # TODO: impl ssl
        self.app = web.Application()
        self.runner = web.AppRunner(self.app)

    async def start(self, routes: List[Route]):
        for route in routes:
            self.app.add_routes([web.get(route.path, route.handler)])

        await self.runner.setup()
        site = web.TCPSite(
            self.runner, host=self.host, port=self.port, ssl_context=self.ssl_context
        )
        await site.start()

    async def serve(self, routes: List[Route]):
        await self.start(routes)
        names = sorted(str(s.name) for s in self.runner.sites)
        print(
            "======== Running on {} ========\n"
            "(Press CTRL+C to quit)".format(", ".join(names))
        )
        try:
            while True:
                await asyncio.sleep(3600)  # sleep forever by 1 hour intervals
        finally:
            await self.runner.cleanup()
