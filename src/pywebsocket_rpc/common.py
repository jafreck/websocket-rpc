from typing import Awaitable, Callable, NamedTuple

IncomingRequestHandler = Callable[[bytes], Awaitable[bytes]]


class Token(NamedTuple):
    value: str
