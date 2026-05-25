import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dispatchr.outbox import EventPublisher, MessageSerializer, OutboxStorage


class OutboxProcessor:
    def __init__(
        self,
        publisher: "EventPublisher",
        storage: "OutboxStorage",
        serializer: "MessageSerializer",
        batch_size: int = 100,
    ):
        self.publisher = publisher
        self.storage = storage
        self.serializer = serializer
        self.batch_size = batch_size

    async def process_batch(self) -> int:
        messages = await self.storage.get_pending_messages(self.batch_size)
        if not messages:
            return 0

        for msg in messages:
            event = self.serializer.deserialize(msg.message_type, msg.payload)
            await self.publisher.publish(event)

        await self.storage.mark_as_published([m.id for m in messages])
        return len(messages)


class OutboxWorker:
    def __init__(self, processor: OutboxProcessor, poll_interval: float = 1.0):
        self.processor = processor
        self.poll_interval = poll_interval
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            await self._task

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            processed = await self.processor.process_batch()
            if processed == 0:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
                except asyncio.TimeoutError:
                    pass
