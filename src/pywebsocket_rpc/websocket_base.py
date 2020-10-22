import asyncio
from typing import Dict

from aiohttp import WSMsgType, web, WSCloseCode
import aiojobs
from .common import IncomingRequestHandler
from .proto.gen.node_pb2 import (
    Direction,
    NodeMessage,
    NodeMessageCompleteResponse,
)


class WebsocketBase:
    def __init__(
        self,
        websocket: web.WebSocketResponse,
        incoming_direction: Direction,
        incoming_request_handler: IncomingRequestHandler,
        scheduler: aiojobs.Scheduler,
    ):
        self._ws = websocket
        self.incoming_direction = incoming_direction
        self.incoming_request_handler = incoming_request_handler
        self._scheduler = scheduler

        self.request_dict = {}  # type:  Dict[str, asyncio.Queue]
        self._receive_task = None  # type: asyncio.Task

        self._name = (
            "server_client"
            if self.incoming_direction == Direction.NodeToServer
            else "client"
        )

    async def initialize(self) -> None:
        self._receive_task = await self._scheduler.spawn(self._receive_messages())

    async def recieve_messages(self) -> None:
        await self._receive_task.wait()

    async def _receive_messages(self) -> None:
        print(f"{self._name} starting to receive messages")
        try:
            print(
                f"{self._name}: self._ws is not None={self._ws is not None} and not self._ws.closed={not self._ws.closed} and not self._receive_task.closed={not self._receive_task.closed}"
            )
            while (
                self._ws is not None
                and not self._ws.closed
                and not self._receive_task.closed
            ):
                ws_msg = await self._ws.receive()
                print(f"{self._name} ws_msg={ws_msg}")
                if ws_msg.type == WSMsgType.BINARY:
                    node_msg = NodeMessage()
                    node_msg.ParseFromString(ws_msg.data)
                    print(
                        f"{self._name} received node_msg: direction={node_msg.direction}, id={node_msg.id}"
                    )
                    if node_msg.direction == self.incoming_direction:
                        response = await self.incoming_request_handler(
                            node_msg.fullRequest.bytes
                        )
                        # currently, only bytes change in responses, so just reuse node_msg
                        node_msg.fullResponse.CopyFrom(
                            NodeMessageCompleteResponse(bytes=response)
                        )
                        await self._ws.send_bytes(node_msg.SerializeToString())
                    else:  # outgoing request
                        response_queue = self.request_dict[node_msg.id]
                        response_queue.put_nowait(node_msg)
                elif ws_msg.type == WSMsgType.ERROR:
                    print(f"{self._name} recieved error message")
                    break
                elif ws_msg.type == WSMsgType.CLOSED:
                    print(f"{self._name} recieved closed message, data={ws_msg.data}")
                    break
                elif ws_msg.type == WSMsgType.CLOSE:
                    print(
                        f"{self._name} recieved close message, data={WSCloseCode(ws_msg.data).name}"
                    )
                    break
                elif ws_msg.type == WSMsgType.CLOSING:
                    print(f"{self._name} recieved closing message, data={ws_msg.data}")
                    break
                # TODO: handle all other message types
        except asyncio.CancelledError:
            print(f"{self._name}: {self._receive_messages.__name__} cancelled")
            raise
        except Exception as e:
            print(
                f"{self._name}: {self._receive_messages.__name__} encountered exception (type={type(e)}): {e}"
            )
            raise

    async def request(self, node_msg: NodeMessage) -> bytes:
        await self._ws.send_bytes(node_msg.SerializeToString())
        response_queue = asyncio.Queue()
        self.request_dict[node_msg.id] = response_queue

        try:
            response = await asyncio.wait_for(response_queue.get(), 2)
            print(
                f"{self._name} received response: {response}, request_dict={self.request_dict.keys()}"
            )
            return response.fullResponse.bytes
        finally:
            del self.request_dict[node_msg.id]
