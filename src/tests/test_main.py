import asyncio
import functools
import os
import ssl
import sys
import uuid
from typing import List, Dict, Tuple

import aiohttp
import pytest
from aiohttp import WSMsgType, web

# TODO: not sure why pytest is complaining so much about imports,
# but changing sys.path before local imports fixes the issue for now
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/../pywebsocket_rpc"))

import pywebsocket_rpc.client
import pywebsocket_rpc.common
import pywebsocket_rpc.proto.gen.node_pb2
import pywebsocket_rpc.server


def log_test_details(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        print(f"Running {func.__name__}")
        await func(*args, **kwargs)
        print(f"Finished running {func.__name__}")

    return wrapper


async def basic_websocket_request_responder(
    request: web.Request,
    ws: web.WebSocketResponse,
) -> None:
    """
    A websocket route handler that responds to a request byte string, x,
    with "x/answer"
    """
    async for msg in ws:
        print(f"server received message: type={msg.type}, data={msg.data}")
        if msg.type == WSMsgType.BINARY:
            node_msg = pywebsocket_rpc.proto.gen.node_pb2.NodeMessage()
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


async def empty_incoming_request_handler(data: bytes) -> None:
    pass


async def simple_incoming_message_handler(data: bytes) -> bytes:
    return data + b"/answer"


def generate_tokens(count: int) -> List[pywebsocket_rpc.common.Token]:
    return [
        (str(uuid.uuid4()), pywebsocket_rpc.common.Token(value=str(uuid.uuid4())))
        for _ in range(count)
    ]


async def validate_token_hook(
    self: pywebsocket_rpc.server.WebsocketServer, request: web.Request
) -> None:
    client_token = request.headers["Authentication"]
    client_id = request.headers["x-ms-node-id"]

    if self.tokens[client_id].value != client_token:
        raise Exception(f"invalid token: {client_token}")


async def run_test_server(
    port: int,
    host: str = "localhost",
    ssl_context: ssl.SSLContext = None,
    routes: List[pywebsocket_rpc.server.Route] = None,
    tokens: List[Tuple[str, pywebsocket_rpc.common.Token]] = None,
):
    print(f"Running test server on {host}:{port}, ssl={ssl_context is not None}")
    if routes is None:
        routes = [
            pywebsocket_rpc.server.Route(
                path="/ws", handler=basic_websocket_request_responder
            )
        ]
    server = pywebsocket_rpc.server.WebsocketServer(
        host=host,
        port=port,
        ssl_context=ssl_context,
        tokens=dict(tokens) if tokens else {},
    )
    return await server.start(routes)


async def connect_test_client(
    port: int,
    host="localhost",
    path="/ws",
    ssl_context: ssl.SSLContext = None,
    token: Tuple[str, pywebsocket_rpc.common.Token] = None,
    incoming_request_handler: pywebsocket_rpc.common.IncomingRequestHandler = None,
) -> pywebsocket_rpc.client.WebsocketClient:
    protocol = "https" if ssl_context is not None else "http"

    if incoming_request_handler is None:
        incoming_request_handler = empty_incoming_request_handler

    client = pywebsocket_rpc.client.WebsocketClient(
        connect_address=f"{protocol}://{host}:{port}{path}",
        incoming_request_handler=incoming_request_handler,
        ssl_context=ssl_context,
        token=token[1] if token else None,
        id=token[0] if token else None,
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
            pywebsocket_rpc.server.Route(
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
    tokens = generate_tokens(1)
    await run_test_server(port=port, ssl_context=server_ssl_ctx, tokens=tokens)
    async with pywebsocket_rpc.client.WebsocketClient(
        connect_address=f"https://localhost:{port}/ws",
        incoming_request_handler=empty_incoming_request_handler,
        token=tokens[0][1],
        id=tokens[0][0],
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
        request: web.Request,
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
        return ws

    await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(path="/ws", handler=drop_connection_handler)
        ],
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
        request: web.Request,
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
        nonlocal connection_count
        print(f"drop_first_connection_handler: connection_count={connection_count}")
        if connection_count == 0:
            print("dropping first connection")
            connection_count += 1
            return ws

        await basic_websocket_request_responder(request, ws)

        print("websocket connection closed")
        return ws

    await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(
                path="/ws", handler=drop_first_connection_handler
            )
        ],
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
    tokens = generate_tokens(1)
    await run_test_server(port=port, ssl_context=server_ssl_ctx, tokens=tokens)

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

    await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(
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
            token=generate_tokens(1)[0],  # generate random Token
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
async def test_server_generated_request_success(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    s_client = None  # type: pywebsocket_rpc.server.ServerClient

    async def server_client_handler(
        request: web.Request,
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
        nonlocal s_client
        client_id = request.headers["x-ms-node-id"]
        s_client = pywebsocket_rpc.server.ServerClient(
            id=client_id,
            websocket=ws,
            incoming_request_handler=empty_incoming_request_handler,
        )
        s_client.initialize()

        await s_client.receive_messages()
        return ws

    tokens = generate_tokens(1)
    await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(path="/ws", handler=server_client_handler)
        ],
        tokens=tokens,
    )

    await connect_test_client(
        port=port,
        ssl_context=client_ssl_ctx,
        token=tokens[0],  # use valid token
        incoming_request_handler=simple_incoming_message_handler,
    )

    assert b"test/answer" == await s_client.request(b"test")


@log_test_details
async def test_concurrent_multi_generated_request_success(
    port: int, server_ssl_ctx: ssl.SSLContext, client_ssl_ctx: ssl.SSLContext
):
    s_clients = []  # type: List[pywebsocket_rpc.server.ServerClient]

    async def server_client_handler(
        request: web.Request,
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
        nonlocal s_clients
        client_id = request.headers["x-ms-node-id"]
        s_client = pywebsocket_rpc.server.ServerClient(
            websocket=ws,
            incoming_request_handler=empty_incoming_request_handler,
            id=client_id,
        )
        s_client.initialize()
        s_clients.append(s_client)

        await s_client.receive_messages()
        return ws

    tokens = generate_tokens(10)
    await run_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(
                path="/ws",
                handler=server_client_handler,
                pre_prepare_hook=validate_token_hook,
            )
        ],
        tokens=tokens,
    )

    for i in range(10):
        await connect_test_client(
            port=port,
            ssl_context=client_ssl_ctx,
            token=tokens[i],  # use valid token
            incoming_request_handler=simple_incoming_message_handler,
        )

    request_tasks = []

    for s_client in s_clients:
        request_tasks.append(s_client.request(f"test{s_client.id}".encode()))

    responses = await asyncio.gather(*request_tasks, return_exceptions=True)

    for i, response in enumerate(responses):
        s_client = s_clients[i]
        assert f"test{s_client.id}/answer".encode() == response
