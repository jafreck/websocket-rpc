import functools
import os
import ssl
import sys
import uuid
from typing import List, Tuple

import logging

import aiohttp
from aiohttp import WSMsgType, web

from test_proto.gen.test_pb2 import NodeHttpRequest, NodeHttpResponse

# TODO: not sure why pytest is complaining so much about imports,
# but changing sys.path before local imports fixes the issue for now
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/../pywebsocket_rpc"))

import pywebsocket_rpc.client
import pywebsocket_rpc.common
import pywebsocket_rpc.proto.gen.node_pb2
import pywebsocket_rpc.server

log = logging.getLogger(__name__)


def log_test_details(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        log.info(f"Running {func.__name__}")
        await func(*args, **kwargs)
        log.info(f"Finished running {func.__name__}")

    return wrapper


async def basic_websocket_request_responder(
    self,
    request: web.Request,
    ws: web.WebSocketResponse,
) -> None:
    """
    A websocket route handler that responds to a request byte string, x,
    with "x/answer"
    """
    log_key = __name__
    async for msg in ws:
        log.info(f"{log_key} received message: type={msg.type}, data={msg.data}")
        if msg.type == WSMsgType.BINARY:
            node_msg = pywebsocket_rpc.proto.gen.node_pb2.NodeMessage()
            node_msg.ParseFromString(msg.data)
            log.info(f"server received node_msg: {node_msg}")
            node_msg.fullResponse.CopyFrom(
                pywebsocket_rpc.proto.gen.node_pb2.NodeMessageCompleteResponse(
                    bytes=node_msg.fullRequest.bytes + b"/answer"
                )
            )
            await ws.send_bytes(node_msg.SerializeToString())

        elif msg.type == WSMsgType.ERROR:
            log.info(f"{log_key} received error, closing")
            break
        elif msg.type == aiohttp.WSMsgType.CLOSED:
            log.info(f"{log_key} received closed, closing")
            break
        elif msg.type == aiohttp.WSMsgType.CLOSING:
            log.info(f"{log_key} received closing, closing")
            break

    log.info(f"{log_key} done receiving messages")


async def empty_incoming_request_handler(data: bytes) -> None:
    pass


async def simple_incoming_message_handler(data: bytes) -> bytes:
    return data + b"/answer"


def generate_tokens(count: int) -> List[Tuple[str, pywebsocket_rpc.common.Token]]:
    return [
        (
            str(uuid.uuid4()),
            pywebsocket_rpc.common.Token.create_token(cluster_id=str(uuid.uuid4())),
        )
        for _ in range(count)
    ]


async def validate_token_hook(
    self: pywebsocket_rpc.server.WebsocketServer, request: web.Request
) -> None:
    client_token = request.headers["Authentication"]
    client_id = request.headers["x-ms-nodeidentifier"]

    if self.tokens[client_id].value != client_token:
        raise Exception(f"invalid token: {client_token}")


def get_test_server(
    port: int,
    host: str = "localhost",
    ssl_context: ssl.SSLContext = None,
    routes: List[pywebsocket_rpc.server.Route] = None,
    tokens: List[Tuple[str, pywebsocket_rpc.common.Token]] = None,
) -> pywebsocket_rpc.server.WebsocketServer:
    log.info(f"Running test server on {host}:{port}, ssl={ssl_context is not None}")
    if routes is None:
        routes = [
            pywebsocket_rpc.server.Route(
                path="/ws", handler=basic_websocket_request_responder
            )
        ]

    return pywebsocket_rpc.server.WebsocketServer(
        host=host,
        port=port,
        ssl_context=ssl_context,
        tokens=dict(tokens) if tokens else {},
        routes=routes,
    )


def get_test_client(
    port: int,
    host="localhost",
    path="/ws",
    ssl_context: ssl.SSLContext = None,
    token: Tuple[str, pywebsocket_rpc.common.Token] = None,
    incoming_request_handler: pywebsocket_rpc.common.IncomingRequestHandler = None,
):
    protocol = "https" if ssl_context is not None else "http"
    if incoming_request_handler is None:
        incoming_request_handler = empty_incoming_request_handler

    return pywebsocket_rpc.client.WebsocketClient(
        connect_address=f"{protocol}://{host}:{port}{path}",
        incoming_request_handler=incoming_request_handler,
        ssl_context=ssl_context,
        token=token[1] if token else None,
        id=token[0] if token else None,
    )


class MultiClientManager:
    def __init__(
        self,
        port: int,
        host="localhost",
        path="/ws",
        ssl_context: ssl.SSLContext = None,
        tokens: List[Tuple[str, pywebsocket_rpc.common.Token]] = None,
        incoming_request_handler: pywebsocket_rpc.common.IncomingRequestHandler = None,
    ):
        self.port = port
        self.host = host
        self.path = path
        self.ssl_context = ssl_context
        self.tokens = tokens
        self.incoming_request_handler = incoming_request_handler

        self.clients = []

    async def __aenter__(self):
        for token in self.tokens:
            client = get_test_client(
                port=self.port,
                host=self.host,
                path=self.path,
                ssl_context=self.ssl_context,
                token=token,
                incoming_request_handler=self.incoming_request_handler,
            )
            await client.__aenter__()
            self.clients.append(client)

    async def __aexit__(self, exc_type, exc, tb):
        for client in self.clients:
            await client.__aexit__(exc_type, exc, tb)

        if exc is not None:
            raise exc
