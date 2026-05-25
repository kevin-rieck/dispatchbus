import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest

from dispatchr import EventBase
from dispatchr.outbox import JSONSerializer, OutboxMessage, OutboxProcessor, OutboxWorker


@dataclass(frozen=True)
class DummyEvent(EventBase):
    message_name = "dummy"


def test_outbox_retention_policy_is_exported():
    from dispatchr.outbox import OutboxRetentionPolicy

    policy = OutboxRetentionPolicy(retention_period=timedelta(days=30), max_published_rows=500)

    assert policy.retention_period == timedelta(days=30)
    assert policy.max_published_rows == 500


@pytest.mark.parametrize(
    ("retention_period", "max_published_rows"),
    [
        (timedelta(days=-1), None),
        (timedelta(days=1), -1),
    ],
)
def test_outbox_retention_policy_rejects_invalid_values(
    retention_period: timedelta, max_published_rows: int | None
):
    from dispatchr.outbox import OutboxRetentionPolicy

    with pytest.raises(ValueError):
        OutboxRetentionPolicy(
            retention_period=retention_period, max_published_rows=max_published_rows
        )


@pytest.mark.parametrize("eviction_interval", [0, -0.1])
def test_outbox_worker_rejects_non_positive_eviction_interval(eviction_interval: float):
    from dispatchr.outbox import OutboxRetentionPolicy

    storage = MockStorage()
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})
    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)

    with pytest.raises(ValueError, match="eviction_interval"):
        OutboxWorker(
            processor=processor,
            retention_policy=OutboxRetentionPolicy(retention_period=timedelta(days=30)),
            eviction_interval=eviction_interval,
        )


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
        self.eviction_calls = []
        self.raise_on_evict = False

    async def claim_pending_messages(self, batch_size: int):
        claimed = self.messages[:batch_size]
        self.messages = self.messages[batch_size:]
        return claimed

    async def mark_as_published(self, message_ids: list[str]):
        self.published.extend(message_ids)

    async def release_claims(self, message_ids: list[str]):
        self.released.extend(message_ids)

    async def evict_published_messages(self, policy):
        self.eviction_calls.append(policy)
        if self.raise_on_evict:
            raise RuntimeError("eviction failed")
        return 0


class MockPublisher:
    def __init__(self):
        self.published = []

    async def publish(self, event):
        self.published.append(event)


class FailingClaimStorage(MockStorage):
    async def claim_pending_messages(self, batch_size: int):
        raise RuntimeError("claim failed")


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


@pytest.mark.asyncio
async def test_outbox_worker_does_not_evict_when_retention_is_disabled():
    storage = MockStorage()
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})
    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    worker = OutboxWorker(processor=processor, poll_interval=0.05)

    worker.start()
    await asyncio.sleep(0.15)
    await worker.stop()

    assert storage.eviction_calls == []


@pytest.mark.asyncio
async def test_outbox_worker_runs_eviction_on_configured_interval():
    from dispatchr.outbox import OutboxRetentionPolicy

    storage = MockStorage()
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})
    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    worker = OutboxWorker(
        processor=processor,
        poll_interval=0.05,
        retention_policy=OutboxRetentionPolicy(
            retention_period=timedelta(days=30), max_published_rows=100
        ),
        eviction_interval=0.05,
    )

    worker.start()
    await asyncio.sleep(0.2)
    await worker.stop()

    assert len(storage.eviction_calls) >= 1
    assert storage.eviction_calls[0].retention_period == timedelta(days=30)
    assert storage.eviction_calls[0].max_published_rows == 100


@pytest.mark.asyncio
async def test_outbox_worker_continues_running_when_eviction_fails():
    from dispatchr.outbox import OutboxRetentionPolicy

    storage = MockStorage()
    storage.raise_on_evict = True
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})
    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    worker = OutboxWorker(
        processor=processor,
        poll_interval=0.05,
        retention_policy=OutboxRetentionPolicy(retention_period=timedelta(days=30)),
        eviction_interval=0.05,
    )

    worker.start()
    await asyncio.sleep(0.2)
    await worker.stop()

    assert len(storage.eviction_calls) >= 1
    assert storage.published == ["1"]


@pytest.mark.asyncio
async def test_outbox_worker_retries_eviction_without_waiting_full_interval_after_failure():
    from dispatchr.outbox import OutboxRetentionPolicy

    storage = MockStorage()
    storage.raise_on_evict = True
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})
    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    worker = OutboxWorker(
        processor=processor,
        poll_interval=0.05,
        retention_policy=OutboxRetentionPolicy(retention_period=timedelta(days=30)),
        eviction_interval=1.0,
    )

    worker.start()
    await asyncio.sleep(0.2)
    await worker.stop()

    assert len(storage.eviction_calls) >= 2


@pytest.mark.asyncio
async def test_outbox_worker_runs_eviction_even_when_processing_fails():
    from dispatchr.outbox import OutboxRetentionPolicy

    storage = FailingClaimStorage()
    publisher = MockPublisher()
    serializer = JSONSerializer({"dummy": DummyEvent})
    processor = OutboxProcessor(publisher=publisher, storage=storage, serializer=serializer)
    worker = OutboxWorker(
        processor=processor,
        poll_interval=0.05,
        retention_policy=OutboxRetentionPolicy(retention_period=timedelta(days=30)),
        eviction_interval=0.05,
    )

    worker.start()
    await asyncio.sleep(0.2)
    await worker.stop()

    assert len(storage.eviction_calls) >= 1
