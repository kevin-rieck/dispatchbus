import json
from dataclasses import dataclass

from dispatchr import EventBase
from dispatchr.outbox import JSONSerializer


@dataclass(frozen=True)
class UserCreated(EventBase):
    message_name = "user.created"
    user_id: int


def test_json_serializer():
    serializer = JSONSerializer({UserCreated.message_name: UserCreated})
    event = UserCreated(user_id=1)

    payload = serializer.serialize(event)
    assert json.loads(payload.decode()) == {"user_id": 1}

    deserialized = serializer.deserialize("user.created", payload)
    assert isinstance(deserialized, UserCreated)
    assert deserialized.user_id == 1
