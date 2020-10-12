import uuid
from typing import Awaitable, Callable, NamedTuple

from .proto.gen.node_pb2 import Direction, NodeMessage

IncomingRequestHandler = Callable[[bytes], Awaitable[bytes]]


class Token(NamedTuple):
    value: str


def _contstruct_node_message(
    bytes: bytes, direction: Direction, id=None
) -> NodeMessage:
    node_msg = NodeMessage()
    if id is None:
        node_msg.id = str(uuid.uuid4())
    else:
        node_msg.id = id
    node_msg.bytes = bytes
    node_msg.direction = direction

    return node_msg
