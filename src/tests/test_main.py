import asyncio
import functools
import os
import ssl
import sys
from typing import List

import aiohttp
import pytest
from aiohttp import WSMsgType, web

# TODO: not sure why pytest is complaining so much about imports,
# but changing sys.path before local imports fixes the issue for now
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/../rpc"))

import rpc.client
import rpc.main
import rpc.proto.gen.node_pb2
import rpc.server

LISTEN_PORT = 1234


@pytest.fixture
def server_port() -> int:
    global LISTEN_PORT
    LISTEN_PORT += 1
    return LISTEN_PORT


def async_wrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        coro = asyncio.coroutine(func)
        future = coro(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)

    return wrapper


async def basic_websocket_request_responder(ws: web.WebSocketResponse) -> None:
    async for msg in ws:
        print(f"server received message: type={msg.type}, data={msg.data}")
        if msg.type == WSMsgType.BINARY:
            node_msg = rpc.proto.gen.node_pb2.NodeMessage()
            node_msg.ParseFromString(msg.data)
            print(f"server received node_msg: {node_msg}")
            node_msg.bytes = node_msg.bytes + b"/answer"
            await ws.send_bytes(node_msg.SerializeToString())

        elif msg.type == WSMsgType.ERROR:
            print("simple_websocket_test_handler received error, closing")
            break
        elif msg.type == aiohttp.WSMsgType.CLOSED:
            print("simple_websocket_test_handler received closed, closing")
            break


async def simple_websocket_test_handler(request: web.Request) -> web.WebSocketResponse:
    wsr = web.WebSocketResponse()
    await wsr.prepare(request)
    await basic_websocket_request_responder(wsr)
    print("websocket connection closed")
    return wsr


async def run_test_server(
    port: int,
    host: str = "localhost",
    ssl_context: ssl.SSLContext = None,
    routes: List[rpc.server.Route] = None,
):
    print("running test server")
    if routes is None:
        routes = [rpc.server.Route("/ws", simple_websocket_test_handler)]
    server = rpc.main.WebsocketServer(host=host, port=port, ssl_context=ssl_context)
    await server.start(routes)


async def connect_test_client(
    port: int, host="localhost", path="/ws", ssl_context: ssl.SSLContext = None
) -> rpc.client.WebsocketClient:
    protocol = "https" if ssl_context is not None else "http"
    client = rpc.main.WebsocketClient(
        f"{protocol}://{host}:{port}{path}", ssl_context=ssl_context
    )
    # await client.connect()
    return client


####################################
#            happy path            #
####################################


async def test_simple_client_server_no_ssl(server_port: int):
    await run_test_server(port=server_port, ssl_context=None)
    client = await connect_test_client(port=server_port, ssl_context=None)

    response = await client.send_and_receive(b"test")

    assert response == b"test/answer"


async def test_simple_client_server_with_ssl(
    server_port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    await run_test_server(port=server_port, ssl_context=server_ssl_ctx)
    client = await connect_test_client(port=server_port, ssl_context=client_ssl_ctx)

    response = await client.send_and_receive(b"test")

    assert response == b"test/answer"


async def test_two_client_requests_correct_response(
    server_port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    await run_test_server(port=server_port, ssl_context=server_ssl_ctx)
    client = await connect_test_client(port=server_port, ssl_context=client_ssl_ctx)

    response1_task = client.send_and_receive(b"test")
    response2_task = client.send_and_receive(b"test2")

    results = await asyncio.gather(response1_task, response2_task)

    assert results[0] == b"test/answer"
    assert results[1] == b"test2/answer"


async def test_websocket_connection_multiple_concurrent_requests_success(
    server_port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    await run_test_server(
        port=server_port,
        ssl_context=server_ssl_ctx,
        routes=[rpc.server.Route("/ws", simple_websocket_test_handler)],
    )
    client = await connect_test_client(port=server_port, ssl_context=client_ssl_ctx)

    results = await asyncio.gather(
        client.send_and_receive(b"test0"),
        client.send_and_receive(b"test1"),
        client.send_and_receive(b"test2"),
    )
    for i, result in enumerate(results):
        assert result == f"test{i}/answer".encode("utf-8")


####################################
#         failure scenarios        #
####################################


async def test_dropped_websocket_connection_times_out(
    server_port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    async def drop_connection_handler(
        request: web.Request,
    ) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        return ws

    await run_test_server(
        port=server_port,
        ssl_context=server_ssl_ctx,
        routes=[rpc.server.Route("/ws", drop_connection_handler)],
    )
    client = await connect_test_client(port=server_port, ssl_context=client_ssl_ctx)

    with pytest.raises(asyncio.TimeoutError):
        await client.send_and_receive(b"test")


async def test_websocket_reconnect_after_connection_lost(
    server_port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    connection_count = 0

    async def drop_first_connection_handler(
        request: web.Request,
    ) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        nonlocal connection_count
        print(f"drop_first_connection_handler: connection_count={connection_count}")
        if connection_count == 0:
            print("dropping first connection")
            connection_count += 1
            return ws

        await basic_websocket_request_responder(ws)

        print("websocket connection closed")
        return ws

    await run_test_server(
        port=server_port,
        ssl_context=server_ssl_ctx,
        routes=[rpc.server.Route("/ws", drop_first_connection_handler)],
    )
    client = await connect_test_client(port=server_port, ssl_context=client_ssl_ctx)

    response1_task = client.send_and_receive(b"test")
    response2_task = client.send_and_receive(b"test2")
    response3_task = client.send_and_receive(b"test3")

    with pytest.raises(asyncio.TimeoutError):
        await response1_task

    results = await asyncio.gather(
        response2_task, response3_task, return_exceptions=True
    )

    assert connection_count == 1
    assert results[0] == b"test2/answer"
    assert results[1] == b"test3/answer"
