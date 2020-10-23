import base64
import json
from typing import Any, Awaitable, Callable, Dict, List, NamedTuple, Union

IncomingRequestHandler = Callable[[bytes], Awaitable[bytes]]
JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

CLUSTER_ID_CLAIM_KEY = "x-ms-clusterid"


class Token(NamedTuple):
    value: str  # base 64 encoded string representing a json object with required claims (x-ms-clusterid)

    @staticmethod
    def create_token(cluster_id: str) -> "Token":
        val = base64.b64encode(
            json.dumps({CLUSTER_ID_CLAIM_KEY: cluster_id}).encode("utf-8")
        ).decode()
        return Token(value=f"Bearer {val}")

    def get_claims(self) -> Dict[str, JSONType]:
        return json.loads(base64.standard_b64decode(self.value.split(" ")[1]))
