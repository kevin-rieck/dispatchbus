import json
from collections.abc import Mapping
from dataclasses import asdict
from typing import Any, cast

from dispatchbus import MessageBase


class JSONSerializer:
    """
    A naive JSON serializer for dispatchbus messages.

    This serializer converts fields to primitive JSON types using Python's built-in `json` module,
    falling back to string representation (`str()`) for complex types like `datetime` or `UUID`.

    Deserialization is naive: it simply unpacks the JSON dictionary (`**data`) into the target
    class.
    Complex types will be passed to the constructor as strings. If you require robust type coercion
    (e.g., parsing strings back into `datetime` or `UUID` objects), you must either use a dataclass
    that coerces types (like Pydantic) or provide your own `MessageSerializer` implementation.
    """

    def __init__(self, message_types: Mapping[str, type[MessageBase]]):
        self.message_types = message_types

    def serialize(self, message: MessageBase) -> bytes:
        return json.dumps(asdict(cast(Any, message)), default=str).encode("utf-8")

    def deserialize(self, message_type: str, payload: bytes) -> MessageBase:
        cls = self.message_types[message_type]
        data = json.loads(payload.decode("utf-8"))
        return cls(**data)
