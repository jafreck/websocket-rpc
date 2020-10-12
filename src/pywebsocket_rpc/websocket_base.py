import asyncio
from typing import Dict

from aiohttp import WSMsgType, web

from .common import IncomingRequestHandler
from .proto.gen.node_pb2 import Direction, NodeMessage


class WebsocketBase:
    def __init__(
        self,
        websocket: web.WebSocketResponse,
        incoming_direction: Direction,
        incoming_request_handler: IncomingRequestHandler,
    ):
        self._ws = websocket
        self.incoming_direction = incoming_direction
        self.incoming_request_handler = incoming_request_handler

        self.request_dict = {}  # type:  Dict[str, asyncio.Queue]
        self._receive_task = None  # type: asyncio.Task

        self._name = (
            "server" if self.incoming_direction == Direction.NodeToServer else "client"
        )

    def initialize(self):
        loop = asyncio.get_event_loop()
        self._receive_task = loop.create_task(self._receive_messages())

    async def recieve_messages(self):
        await self._receive_task

    async def _receive_messages(self):
        print(f"{self._name} starting to receive messages")
        try:
            while self._ws is not None or self._ws.closed or self._receive_task.done():
                ws_msg = await self._ws.receive()
                if ws_msg.type == WSMsgType.BINARY:
                    node_msg = NodeMessage()
                    node_msg.ParseFromString(ws_msg.data)
                    print(
                        f"{self._name} received node_msg: direction={node_msg.direction}, id={node_msg.id}"
                    )
                    if node_msg.direction == self.incoming_direction:
                        response = await self.incoming_request_handler(node_msg.bytes)
                        # currently, only bytes change in responses, so just reuse node_msg
                        node_msg.bytes = response
                        await self._ws.send_bytes(node_msg.SerializeToString())
                    else:  # outgoing request
                        response_queue = self.request_dict[node_msg.id]
                        response_queue.put_nowait(node_msg)
                elif ws_msg.type == WSMsgType.ERROR:
                    break
                elif ws_msg.type == WSMsgType.CLOSED:
                    break
                # TODO: handle all other message types
        except asyncio.CancelledError:
            print(f"{self._name}: {self._receive_messages.__name__} cancelled")
            raise
        except Exception as e:
            print(
                f"{self._name}: {self._receive_messages.__name__} encountered exception (type={type(e)}): {e}"
            )

    async def request(self, node_msg: NodeMessage) -> bytes:
        await self._ws.send_bytes(node_msg.SerializeToString())
        response_queue = asyncio.Queue()
        self.request_dict[node_msg.id] = response_queue

        try:
            response = await asyncio.wait_for(response_queue.get(), 2)
            print(
                f"{self._name} received response: {response}, request_dict={self.request_dict.keys()}"
            )
            return response.bytes
        finally:
            del self.request_dict[node_msg.id]
