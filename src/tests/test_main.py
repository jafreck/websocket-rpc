import asyncio
import functools
import os
import ssl
import sys
from multiprocessing import Process
from typing import Any, Optional, List

import aiohttp
import pytest
import trustme
from aiohttp import WSMsgType, request, web

# TODO: not sure why pytest is complaining so much about imports,
# but changing sys.path before local imports fixes the issue for now
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/../rpc"))

import rpc.client
import rpc.main
import rpc.proto.gen.node_pb2
import rpc.server

listen_port = 1234


@pytest.fixture
def port() -> int:
    global listen_port
    listen_port += 1
    return listen_port


def async_wrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        coro = asyncio.coroutine(func)
        future = coro(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)

    return wrapper


async def websocket_test_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        print(f"received message: type={msg.type}, data={msg.data}")
        if msg.type == WSMsgType.BINARY:
            node_msg = rpc.proto.gen.node_pb2.NodeMessage()
            node_msg.ParseFromString(msg.data)
            print(f"received node_msg: {node_msg}")
            node_msg.bytes = node_msg.bytes + b"/answer"
            await ws.send_bytes(node_msg.SerializeToString())

        elif msg.type == WSMsgType.ERROR:
            print(f"websocket_test_handler received error, closing")
            break
        elif msg.type == aiohttp.WSMsgType.CLOSED:
            print(f"websocket_test_handler received closed, closing")
            break

    print("websocket connection closed")
    return ws


async def run_test_server(
    port: int,
    host: str = "localhost",
    ssl_context: ssl.SSLContext = None,
    routes: List[rpc.server.Route] = None,
):
    print("running test server")
    if routes == None:
        routes = [rpc.server.Route("/ws", websocket_test_handler)]
    server = rpc.main.WebsocketServer(host=host, port=port, ssl_context=ssl_context)
    await server.start(routes)


async def connect_test_client(
    port: int, host="localhost", ssl_context: ssl.SSLContext = None
) -> rpc.client.WebsocketClient:
    protocol = "https" if ssl_context is not None else "http"
    client = rpc.main.WebsocketClient(
        f"{protocol}://localhost:{port}/ws", ssl_context=ssl_context
    )
    await client.connect()
    return client


async def test_simple_client_server_no_ssl(port: int):
    await run_test_server(port=port, ssl_context=None)
    client = await connect_test_client(port=port, ssl_context=None)

    response = await client.send_and_receive(b"test")

    assert response == b"test/answer"


async def test_simple_client_server_with_ssl(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    await run_test_server(port=port, ssl_context=server_ssl_ctx)
    client = await connect_test_client(port=port, ssl_context=client_ssl_ctx)

    response = await client.send_and_receive(b"test")

    assert response == b"test/answer"


async def test_two_client_requests_correct_response(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    await run_test_server(port=port, ssl_context=server_ssl_ctx)
    client = await connect_test_client(port=port, ssl_context=client_ssl_ctx)

    response1_task = client.send_and_receive(b"test")
    response2_task = client.send_and_receive(b"test2")

    results = await asyncio.gather(response1_task, response2_task)

    assert results[0] == b"test/answer"
    assert results[1] == b"test2/answer"


async def test_websocket_reconnect_after_connection_lost():
    pass
