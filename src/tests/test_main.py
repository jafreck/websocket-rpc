import asyncio
import functools
import os
import ssl
import sys
from multiprocessing import Process
from typing import Optional  # noqa

import aiohttp
import pytest
from aiohttp import WSMsgType, request, web

# TODO: not sure why pytest is complaining so much about imports,
# but changing sys.path before local imports fixes the issue for now
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/../rpc"))

import rpc.client
import rpc.main
import rpc.proto.gen.node_pb2
import rpc.server


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
        if msg.type == WSMsgType.TEXT:
            print("recieved msg:", msg.data)
            if msg.data == "close":
                await ws.close()
            else:
                await ws.send_str(msg.data + "/answer")
        elif msg.type == WSMsgType.ERROR:
            print("ws connection closed with exception %s" % ws.exception())

    print("websocket connection closed")
    return ws


def construct_tls12_restrictive_ssl_context() -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    context.options |= ssl.OP_NO_TLSv1
    context.options |= ssl.OP_NO_TLSv1_1
    context.options |= ssl.OP_NO_SSLv2
    context.options |= ssl.OP_NO_SSLv3
    return context

def construct_ssl_context(
        certpath: str,
        client_certpath: Optional[str] = None
) -> ssl.SSLContext:
    """Construct ssl context to be used for server/client communications

    :param certpath: path to cert chain
    :param client_certpath: path to CA certificates
    to trust for certificate verification
    :return: ssl.SSLContext
    """
    # configure SSL/TLS options cert file paths
    context = construct_tls12_restrictive_ssl_context()
    context.load_cert_chain(certfile=certpath)
    # if client_certpath and common.config.agent_ssl_client_auth():
    #     context.load_verify_locations(
    #     cafile=client_certpath)
    return context

async def run_test_server(host: str, port: int, ssl_context: ssl.SSLContext = None):
    print("running test server")
    server = rpc.main.WebsocketServer(host=host, port=port, ssl_context=ssl_context)
    await server.start([rpc.server.Route("/ws", websocket_test_handler)])
    print("test server shutting down")

async def connect_test_client() -> rpc.client.WebsocketClient:
    client = rpc.main.WebsocketClient('http://127.0.0.1:1234/ws')
    await client.connect()
    return client

async def test_simple_client_server_no_ssl():
    await run_test_server(host="127.0.0.1", port=1234, ssl_context=None)
    client = await connect_test_client()
    await client.send_str("test")
    response = await client.receive_str()

    assert response == "test/answer"
