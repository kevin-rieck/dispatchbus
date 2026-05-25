# tests/test_outbox_protocols.py
from datetime import datetime
from dispatchr.outbox import OutboxMessage, EventPublisher, OutboxStorage, MessageSerializer
from dispatchr import EventBase

class MockEvent(EventBase):
    message_name = "mock.event"

def test_outbox_message_creation():
    msg = OutboxMessage(
        id="123",
        message_type="mock.event",
        payload=b"{}",
        created_at=datetime(2026, 1, 1)
    )
    assert msg.id == "123"
