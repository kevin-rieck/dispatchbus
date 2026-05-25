import json
from collections.abc import Mapping
from dataclasses import asdict

from dispatchr import MessageBase


class JSONSerializer:
    def __init__(self, message_types: Mapping[str, type[MessageBase]]):
        self.message_types = message_types

    def serialize(self, message: MessageBase) -> bytes:
        return json.dumps(asdict(message)).encode("utf-8")

    def deserialize(self, message_type: str, payload: bytes) -> MessageBase:
        cls = self.message_types[message_type]
        data = json.loads(payload.decode("utf-8"))
        return cls(**data)
