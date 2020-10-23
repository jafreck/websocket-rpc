import asyncio
import os
import ssl
import sys
from contextlib import asynccontextmanager
from typing import Callable, List, NamedTuple

import aiohttp
import aiojobs.aiohttp
import pytest
from aiohttp import web

from helpers import (
    basic_websocket_request_responder,
    empty_incoming_request_handler,
    generate_tokens,
    get_test_client,
    MultiClientManager,
    get_test_server,
    log_test_details,
    simple_incoming_message_handler,
    validate_token_hook,
)
from test_proto.gen.test_pb2 import NodeHttpRequest, NodeHttpResponse

# TODO: not sure why pytest is complaining so much about imports,
# but changing sys.path before local imports fixes the issue for now
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/../pywebsocket_rpc"))

import pywebsocket_rpc.client
import pywebsocket_rpc.common
import pywebsocket_rpc.proto.gen.node_pb2
import pywebsocket_rpc.server

####################################
#            happy path            #
####################################


@log_test_details
async def test_simple_client_server_no_ssl(port: int):
    async with get_test_server(
        port=port,
        ssl_context=None,
    ), get_test_client(port=port, ssl_context=None) as client:
        # async with get_test_client(port=port, ssl_context=None)
        # await client.receive_messages()

        response = await client.request(b"test")

    assert response == b"test/answer"


@log_test_details
async def test_simple_client_server_with_ssl(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    async with get_test_server(port=port, ssl_context=server_ssl_ctx):
        async with get_test_client(port=port, ssl_context=client_ssl_ctx) as client:
            response = await client.request(b"test")

    assert response == b"test/answer"


@log_test_details
async def test_two_client_requests_correct_response(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    async with get_test_server(port=port, ssl_context=server_ssl_ctx):
        async with get_test_client(port=port, ssl_context=client_ssl_ctx) as client:
            response1_task = client.request(b"test")
            response2_task = client.request(b"test2")
            results = await asyncio.gather(response1_task, response2_task)

    assert results[0] == b"test/answer"
    assert results[1] == b"test2/answer"


@log_test_details
async def test_websocket_connection_multiple_concurrent_requests_success(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    async with get_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(
                path="/ws",
                handler=basic_websocket_request_responder,
            )
        ],
    ):
        async with get_test_client(port=port, ssl_context=client_ssl_ctx) as client:
            results = await asyncio.gather(
                client.request(b"test0"),
                client.request(b"test1"),
                client.request(b"test2"),
            )

    for i, result in enumerate(results):
        assert result == f"test{i}/answer".encode("utf-8")


async def test_basic_context_manager_client(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    tokens = generate_tokens(1)
    async with get_test_server(
        port=port, ssl_context=server_ssl_ctx, tokens=tokens
    ), pywebsocket_rpc.client.WebsocketClient(
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
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    async def drop_connection_handler(
        request: web.Request,
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
        return ws

    async with get_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(path="/ws", handler=drop_connection_handler)
        ],
    ):
        async with get_test_client(port=port, ssl_context=client_ssl_ctx) as client:
            with pytest.raises(asyncio.TimeoutError):
                await client.request(b"test")


@log_test_details
async def test_websocket_reconnect_after_connection_lost(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    connection_count = 0

    async def drop_first_connection_handler(
        self,
        request: web.Request,
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
        nonlocal connection_count
        self.log.info(
            f"drop_first_connection_handler: connection_count={connection_count}"
        )
        if connection_count == 0:
            self.log.info("dropping first connection")
            connection_count += 1
            return ws

        await basic_websocket_request_responder(self, request, ws)

        self.log.info("websocket connection closed")
        return ws

    async with get_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(
                path="/ws", handler=drop_first_connection_handler
            )
        ],
    ):
        async with get_test_client(port=port, ssl_context=client_ssl_ctx) as client:
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


def test_token_creation_and_claims():
    token = pywebsocket_rpc.common.Token.create_token("mycluster")
    claims_mapping = token.get_claims()

    assert token.value.split(" ")[0] == "Bearer"
    assert claims_mapping[pywebsocket_rpc.common.CLUSTER_ID_CLAIM_KEY] == "mycluster"


@log_test_details
async def test_valid_token_accepted(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    tokens = generate_tokens(1)
    async with get_test_server(port=port, ssl_context=server_ssl_ctx, tokens=tokens):
        async with get_test_client(
            port=port,
            ssl_context=client_ssl_ctx,
            token=tokens[0],  # use valid token
        ) as client:
            response = await client.request(b"test")

    assert response == b"test/answer"


@log_test_details
async def test_invalid_token_rejected(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    # TODO: this test is ignoring an exception, not sure where...
    async with get_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(
                path="/ws",
                handler=basic_websocket_request_responder,
                pre_prepare_hook=validate_token_hook,
            )
        ],
    ):
        with pytest.raises(aiohttp.WSServerHandshakeError):
            async with get_test_client(
                port=port,
                ssl_context=client_ssl_ctx,
                token=generate_tokens(1)[0],  # generate random Token
            ):
                pass


@log_test_details
async def test_multi_client_valid_token_success(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    pass


####################################
#    server generated requests     #
####################################


@log_test_details
async def test_server_generated_request_success(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    s_client = None  # type: pywebsocket_rpc.server.ServerClient

    async def server_client_handler(
        self,
        request: web.Request,
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
        nonlocal s_client
        client_id = request.headers["x-ms-nodeidentifier"]
        s_client = pywebsocket_rpc.server.ServerClient(
            id=client_id,
            websocket=ws,
            incoming_request_handler=empty_incoming_request_handler,
            scheduler=aiojobs.aiohttp.get_scheduler_from_request(request),
        )
        await s_client.initialize()

        await s_client.receive_messages()
        return ws

    tokens = generate_tokens(1)
    async with get_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(path="/ws", handler=server_client_handler)
        ],
        tokens=tokens,
    ), get_test_client(
        port=port,
        ssl_context=client_ssl_ctx,
        token=tokens[0],  # use valid token
        incoming_request_handler=simple_incoming_message_handler,
    ):
        response = await s_client.request(b"test")

    assert response == b"test/answer"


@log_test_details
async def test_concurrent_multi_generated_request_success(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    s_clients = []  # type: List[pywebsocket_rpc.server.ServerClient]

    async def server_client_handler(
        self,
        request: web.Request,
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:
        nonlocal s_clients
        client_id = request.headers["x-ms-nodeidentifier"]
        s_client = pywebsocket_rpc.server.ServerClient(
            websocket=ws,
            incoming_request_handler=empty_incoming_request_handler,
            id=client_id,
            scheduler=aiojobs.aiohttp.get_scheduler_from_request(request),
        )
        await s_client.initialize()
        s_clients.append(s_client)

        await s_client.receive_messages()
        return ws

    tokens = generate_tokens(10)
    async with get_test_server(
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
    ), MultiClientManager(
        port=port,
        ssl_context=client_ssl_ctx,
        tokens=tokens,  # use valid token
        incoming_request_handler=simple_incoming_message_handler,
    ):
        request_tasks = []

        for s_client in s_clients:
            request_tasks.append(s_client.request(f"test{s_client.id}".encode()))

        responses = await asyncio.gather(*request_tasks, return_exceptions=True)

    assert len(responses) == 10

    for i, response in enumerate(responses):
        s_client = s_clients[i]
        assert f"test{s_client.id}/answer".encode() == response


@log_test_details
async def test_server_generated_request_make_http_request_success(
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    s_client = None  # type: pywebsocket_rpc.server.ServerClient

    async def server_client_handler(
        self,
        request: web.Request,
        ws: web.WebSocketResponse,
    ) -> web.WebSocketResponse:

        nonlocal s_client
        client_id = request.headers["x-ms-nodeidentifier"]
        s_client = pywebsocket_rpc.server.ServerClient(
            id=client_id,
            websocket=ws,
            incoming_request_handler=empty_incoming_request_handler,
            scheduler=aiojobs.aiohttp.get_scheduler_from_request(request),
        )
        self.log.info("initializing s_client")
        await s_client.initialize()

        await s_client.receive_messages()
        self.log.info("server_client_handler done")
        return ws

    async def message_relay_handler(data: bytes) -> bytes:
        node_http_req = NodeHttpRequest()
        node_http_req.ParseFromString(data)

        async with aiohttp.ClientSession() as session:
            try:
                async with session.put(
                    "http://localhost:8080/",
                    data=node_http_req.body,
                    headers=node_http_req.headers,
                ) as resp:
                    node_http_resp = NodeHttpResponse()
                    node_http_resp.status_code = resp.status
                    node_http_resp.body = await resp.read()

                    print(f"node_http_req.headers={node_http_req.headers}")
                    print(f"resp.headers.items()={resp.headers.items()}")
                    for key, value in resp.headers.items():
                        node_http_resp.headers[key] = value

                    return node_http_resp.SerializeToString()

            except Exception as e:
                print(f"put failed with: {e}")

    async def respond_success_handler(request: web.Request):
        try:
            resp = web.Response(
                body=(await request.text() + "/answer").encode(),
                status=200,
                headers={"key2": "value2"},
            )

            return resp
        except Exception as e:
            print(f"exception raised when responding: {e}")
            return web.Response(body="", status=500)

    tokens = generate_tokens(1)
    async with get_test_server(
        port=port,
        ssl_context=server_ssl_ctx,
        routes=[
            pywebsocket_rpc.server.Route(path="/ws", handler=server_client_handler)
        ],
        tokens=tokens,
    ), get_test_client(
        port=port,
        ssl_context=client_ssl_ctx,
        token=tokens[0],  # use valid token
        incoming_request_handler=message_relay_handler,
    ), get_test_webserver(
        port=8080,
        routes=[
            WebRoute(http_method=web.put, path="/", handler=respond_success_handler)
        ],
    ):
        resp_bytes = await s_client.request(
            NodeHttpRequest(headers={"key": "value"}, body=b"test").SerializeToString()
        )

    node_http_resp = NodeHttpResponse()
    node_http_resp.ParseFromString(resp_bytes)
    assert node_http_resp.status_code == 200
    assert node_http_resp.body == b"test/answer"
    assert node_http_resp.headers["key2"] == "value2"


class WebRoute(NamedTuple):
    WebHandler = Callable[[web.Request], web.Response]

    path: str
    handler: WebHandler
    http_method: Callable[[str, WebHandler], None]


@asynccontextmanager
async def get_test_webserver(port: int, routes: List[WebRoute]) -> None:
    app = web.Application()
    for route in routes:
        app.add_routes([route.http_method(route.path, route.handler)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", port)
    await site.start()

    try:
        yield
    finally:
        await runner.cleanup()


async def test_logger(
    caplog,
    port: int,
    server_ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
):
    import logging

    logger = logging.getLogger("pywebsocket_rpc")
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.NullHandler)

    with caplog.at_level(logging.DEBUG):
        tokens = generate_tokens(1)
        async with get_test_server(
            port=port,
            ssl_context=server_ssl_ctx,
            routes=[
                pywebsocket_rpc.server.Route(
                    path="/ws", handler=basic_websocket_request_responder
                )
            ],
            tokens=tokens,
        ), get_test_client(
            port=port,
            ssl_context=client_ssl_ctx,
            token=tokens[0],  # use valid token
            incoming_request_handler=empty_incoming_request_handler,
        ) as client:
            await client.request(b"test")

        # just make sure we are capturing some logs here...
        assert len(caplog.records) > 5
