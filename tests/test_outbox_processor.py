import pytest
from datetime import datetime
from dispatchr import EventBase
from dataclasses import dataclass
from dispatchr.outbox import OutboxMessage, OutboxProcessor, JSONSerializer


@dataclass(frozen=True)
class DummyEvent(EventBase):
    message_name = "dummy"


class MockStorage:
    def __init__(self):
        self.messages = [
            OutboxMessage(id="1", message_type="dummy", payload=b"{}", created_at=datetime.now())
        ]
        self.published = []

    async def get_pending_messages(self, batch_size: int):
        return self.messages[:batch_size]

    async def mark_as_published(self, ids: list[str]):
        self.published.extend(ids)
        self.messages = [m for m in self.messages if m.id not in ids]


class MockPublisher:
    def __init__(self):
        self.published = []

    async def publish(self, event):
        self.published.append(event)


@pytest.mark.asyncio
async def test_outbox_processor():
    storage = MockStorage()
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})

    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    processed = await processor.process_batch()

    assert processed == 1
    assert len(publisher.published) == 1
    assert isinstance(publisher.published[0], DummyEvent)
    assert storage.published == ["1"]
    assert len(storage.messages) == 0
