import argparse
import asyncio
import ssl
import sys
from enum import Enum
from typing import Optional

import aiohttp
from aiohttp import WSMsgType, request, web

import proto.gen.node_pb2
from server import WebsocketServer, Route
from client import WebsocketClient


async def websocket_test_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        print(f"received message: type={msg.type}, data={msg.data}")
        if msg.type == WSMsgType.BINARY:
            node_msg = proto.gen.node_pb2.NodeMessage()
            node_msg.ParseFromString(msg.data)
            print(f"received node_msg: {node_msg}")
            node_msg.bytes = node_msg.bytes + b"/answer"
            await ws.send_bytes(node_msg.SerializeToString())

        elif msg.type == WSMsgType.ERROR:
            print(f"ws connection closed with exception {ws.exception()}")

    print("websocket connection closed")
    return ws


async def run_server():
    # TODO: set up ssl_context ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
    server = WebsocketServer(host="127.0.0.1", port=1234, ssl_context=None)
    await server.serve(routes=[Route("/ws", websocket_test_handler)])


async def run_interactive_client():
    client = WebsocketClient("http://127.0.0.1:1234/ws")
    await client.connect()
    msg = input("client: ").encode("utf-8")
    while True:
        response = await client.send_and_receive(msg)
        print(f"response: {response}")
        msg = input("client: ").encode("utf-8")


async def main():
    if args.mode == Mode.server:
        try:
            await run_server()
        except Exception as ex:
            print(f"Exception caught running server: {ex}")
    elif args.mode == Mode.client:
        try:
            await run_interactive_client()
        except Exception as ex:
            print(f"Exception caught running interactive client: {ex}")
            raise ex


if __name__ == "__main__":

    class Mode(Enum):
        client = "client"
        server = "server"

    parser = argparse.ArgumentParser(description="Run websocket server or client")
    parser.add_argument("mode", type=Mode, help="what mode to run in: client or server")
    args = parser.parse_args()

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
