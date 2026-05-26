import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from dispatchbus import EventBase
from dispatchbus.outbox import JSONSerializer


@dataclass(frozen=True)
class UserCreated(EventBase):
    message_name = "user.created"
    user_id: int
    created_at: datetime
    request_id: uuid.UUID


def test_json_serializer():
    serializer = JSONSerializer({UserCreated.message_name: UserCreated})
    now = datetime.now(UTC)
    req_id = uuid.uuid4()

    event = UserCreated(user_id=1, created_at=now, request_id=req_id)

    payload = serializer.serialize(event)
    expected_dict = {"user_id": 1, "created_at": str(now), "request_id": str(req_id)}
    assert json.loads(payload.decode()) == expected_dict

    deserialized = serializer.deserialize("user.created", payload)
    assert isinstance(deserialized, UserCreated)
    assert deserialized.user_id == 1
    # Check that they are naively deserialized as strings
    assert deserialized.created_at == str(now)  # type: ignore
    assert deserialized.request_id == str(req_id)  # type: ignore
