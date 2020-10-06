import asyncio
import functools
import os
import ssl
import sys
import uuid
from typing import List

import aiohttp
import pytest
from aiohttp import WSMsgType, web

# TODO: not sure why pytest is complaining so much about imports,
# but changing sys.path before local imports fixes the issue for now
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/../rpc"))

import rpc.client
import rpc.common
import rpc.main
import rpc.proto.gen.node_pb2
import rpc.server


def log_test_details(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        print(f"Running {func.__name__}")
        await func(*args, **kwargs)
        print(f"Finished running {func.__name__}")

    return wrapper


async def basic_websocket_request_responder(ws: web.WebSocketResponse) -> None:
    """
    A websocket route handler that responds to a request byte string, x,
    with "x/answer"
    """
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


async def empty_incoming_message_handler(data: bytes) -> None:
    pass


async def run_test_server(
    port: int,
    host: str = "localhost",
    ssl_context: ssl.SSLContext = None,
    routes: List[rpc.server.Route] = None,
) -> List[rpc.common.Token]:
    print(f"Running test server on {host}:{port}, ssl={ssl_context is not None}")
    if routes is None:
        routes = [
            rpc.server.Route(path="/ws", handler=basic_websocket_request_responder)
        ]
    server = rpc.main.WebsocketServer(host=host, port=port, ssl_context=ssl_context)
    return await server.start(routes)


async def connect_test_client(
    port: int,
    host="localhost",
    path="/ws",
    ssl_context: ssl.SSLContext = None,
    token: str = None,
) -> rpc.client.WebsocketClient:
    protocol = "https" if ssl_context is not None else "http"
    client = rpc.main.WebsocketClient(
        connect_address=f"{protocol}://{host}:{port}{path}",
        incoming_message_handler=empty_incoming_message_handler,
        ssl_context=ssl_context,
        token=token,
    )
    await client.connect()
    return client


####################################
#            happy path            #
####################################


@log_test_details
async def test_simple_client_server_no_ssl(port: int):
    await run_test_server(port=port, ssl_context=None)
    client = await connect_test_client(port=port, ssl_context=None)
    response = await client.request(b"test")

    assert response == b"test/answer"


@log_test_details
async def test_simple_client_server_with_ssl(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    await run_test_server(port=port, ssl_context=server_ssl_ctx)
    client = await connect_test_client(port=port, ssl_context=client_ssl_ctx)
    response = await client.request(b"test")

    assert response == b"test/answer"


@log_test_details
async def test_two_client_requests_correct_response(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    await run_test_server(port=port, ssl_context=server_ssl_ctx)
    client = await connect_test_client(port=port, ssl_context=client_ssl_ctx)

    response1_task = client.request(b"test")
    response2_task = client.request(b"test2")
    results = await asyncio.gather(response1_task, response2_task)

    assert results[0] == b"test/answer"
    assert results[1] == b"test2/answer"


@log_test_details
async def test_websocket_connection_multiple_concurrent_requests_success(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            rpc.server.Route(
                path="/ws",
                handler=basic_websocket_request_responder,
            )
        ],
    )
    client = await connect_test_client(port=port, ssl_context=client_ssl_ctx)

    results = await asyncio.gather(
        client.request(b"test0"),
        client.request(b"test1"),
        client.request(b"test2"),
    )
    for i, result in enumerate(results):
        assert result == f"test{i}/answer".encode("utf-8")


async def test_basic_context_manager_client(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    tokens = await run_test_server(port=port, ssl_context=server_ssl_ctx)
    async with rpc.client.WebsocketClient(
        connect_address=f"https://localhost:{port}/ws",
        incoming_message_handler=empty_incoming_message_handler,
        token=tokens[0],
        ssl_context=client_ssl_ctx,
    ) as client:
        response = await client.request(b"test")

    assert response == b"test/answer"


####################################
#         failure scenarios        #
####################################


@log_test_details
async def test_dropped_websocket_connection_times_out(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    async def drop_connection_handler(
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
        return ws

    await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[rpc.server.Route(path="/ws", handler=drop_connection_handler)],
    )
    client = await connect_test_client(port=port, ssl_context=client_ssl_ctx)

    with pytest.raises(asyncio.TimeoutError):
        await client.request(b"test")


@log_test_details
async def test_websocket_reconnect_after_connection_lost(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    connection_count = 0

    async def drop_first_connection_handler(
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
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
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[rpc.server.Route(path="/ws", handler=drop_first_connection_handler)],
    )
    client = await connect_test_client(port=port, ssl_context=client_ssl_ctx)

    response1_task = client.request(b"test")
    response2_task = client.request(b"test2")
    response3_task = client.request(b"test3")

    with pytest.raises(asyncio.TimeoutError):
        await response1_task

    results = await asyncio.gather(
        response2_task, response3_task, return_exceptions=True
    )

    assert connection_count == 1
    assert results[0] == b"test2/answer"
    assert results[1] == b"test3/answer"


def test_graceful_websocket_shutdown_success():
    return None


def test_graceful_websocket_shutdown_client_connects():
    return None


def test_client_autoreconnects_after_connection_dropped():
    return None


####################################
#     authnetication scenarios     #
####################################


@log_test_details
async def test_valid_token_accepted(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    tokens = await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
    )

    print(f"tokens {tokens}")

    client = await connect_test_client(
        port=port,
        ssl_context=client_ssl_ctx,
        token=tokens[0],  # use valid token
    )

    assert await client.request(b"test") == b"test/answer"


@log_test_details
async def test_invalid_token_rejected(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    async def validate_token_hook(
        self: rpc.server.WebsocketServer, request: web.Request
    ) -> None:
        client_token = request.headers["Authentication"]
        if client_token not in self.tokens:
            raise Exception("invalid token")

    await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            rpc.server.Route(
                path="/ws",
                handler=basic_websocket_request_responder,
                pre_prepare_hook=validate_token_hook,
            )
        ],
    )
    with pytest.raises(aiohttp.WSServerHandshakeError):
        await connect_test_client(
            port=port,
            ssl_context=client_ssl_ctx,
            token=rpc.common.Token(str(uuid.uuid4())),  # generate random Token
        )


@log_test_details
async def test_multi_client_valid_token_success(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    pass


####################################
#    server generated requests     #
####################################


@log_test_details
async def server_generated_request_success(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    class ServerClient:
        def __init__(self):
            self.request_dict = {}

        async def request(self, data: bytes) -> bytes:
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

        @staticmethod
        async def server_client_handler(ws: web.WebSocketResponse) -> None:
            """
            A websocket route handler that responds to a request byte string, x,
            with "x/answer"
            """
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

    await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[rpc.server.Route(path="/ws", handler=server_client_handler)],
    )

    client = await connect_test_client(
        port=port,
        ssl_context=client_ssl_ctx,
        token=tokens[0],  # use valid token
    )
