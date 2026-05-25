import asyncio
from dataclasses import dataclass
from datetime import datetime

import pytest

from dispatchr import EventBase
from dispatchr.outbox import JSONSerializer, OutboxMessage, OutboxProcessor, OutboxWorker


@dataclass(frozen=True)
class DummyEvent(EventBase):
    message_name = "dummy"


class MockStorage:
    def __init__(self):
        self.messages = [
            OutboxMessage(
                id="1",
                message_type="dummy",
                payload=b"{}",
                created_at=datetime.now(),
            )
        ]
        self.published = []
        self.released = []

    async def claim_pending_messages(self, batch_size: int):
        claimed = self.messages[:batch_size]
        self.messages = self.messages[batch_size:]
        return claimed

    async def mark_as_published(self, message_ids: list[str]):
        self.published.extend(message_ids)

    async def release_claims(self, message_ids: list[str]):
        self.released.extend(message_ids)


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


@pytest.mark.asyncio
async def test_outbox_processor_empty_batch():
    storage = MockStorage()
    storage.messages = []
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})

    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    processed = await processor.process_batch()

    assert processed == 0
    assert len(publisher.published) == 0
    assert len(storage.published) == 0
    assert len(storage.released) == 0


@pytest.mark.asyncio
async def test_outbox_processor_failure_handling():
    storage = MockStorage()
    storage.messages = [
        OutboxMessage(id="1", message_type="dummy", payload=b"{}", created_at=datetime.now()),
        OutboxMessage(id="2", message_type="dummy", payload=b"{}", created_at=datetime.now()),
    ]

    class FailingPublisher:
        def __init__(self):
            self.published = []

        async def publish(self, event):
            if len(self.published) == 0:
                self.published.append(event)
                raise Exception("Failed on first message")
            self.published.append(event)

    publisher = FailingPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})

    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    processed = await processor.process_batch()

    assert processed == 1
    assert len(publisher.published) == 2
    assert storage.published == ["2"]
    assert storage.released == ["1"]


@pytest.mark.asyncio
async def test_outbox_worker():
    storage = MockStorage()
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})

    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    worker = OutboxWorker(processor=processor, poll_interval=0.1)

    worker.start()
    await asyncio.sleep(0.2)
    await worker.stop()

    assert len(publisher.published) == 1
    assert storage.published == ["1"]
    assert len(storage.messages) == 0


@pytest.mark.asyncio
async def test_outbox_worker_start_raises_if_already_running():
    storage = MockStorage()
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})
    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    worker = OutboxWorker(processor=processor, poll_interval=0.1)

    worker.start()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            worker.start()
    finally:
        await worker.stop()
