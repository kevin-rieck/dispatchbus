import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dispatchr.outbox.models import (
        EventPublisher,
        MessageSerializer,
        OutboxRetentionPolicy,
        OutboxStorage,
    )

logger = logging.getLogger(__name__)


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
        messages = await self.storage.claim_pending_messages(self.batch_size)
        if not messages:
            return 0

        published_ids = []
        failed_ids = []
        for msg in messages:
            try:
                event = self.serializer.deserialize(msg.message_type, msg.payload)
                await self.publisher.publish(event)
                published_ids.append(msg.id)
            except Exception:
                failed_ids.append(msg.id)
                logger.exception("Failed to process message %s", msg.id)

        if published_ids:
            await self.storage.mark_as_published(published_ids)
        if failed_ids:
            await self.storage.release_claims(failed_ids)
        return len(published_ids)


class OutboxWorker:
    def __init__(
        self,
        processor: OutboxProcessor,
        poll_interval: float = 1.0,
        retention_policy: "OutboxRetentionPolicy | None" = None,
        eviction_interval: float | None = None,
    ):
        if eviction_interval is not None and eviction_interval <= 0:
            raise ValueError("eviction_interval must be positive")

        self.processor = processor
        self.poll_interval = poll_interval
        self.retention_policy = retention_policy
        self.eviction_interval = eviction_interval
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._last_eviction_run = 0.0

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            raise RuntimeError("OutboxWorker is already running")

        logger.info("Starting OutboxWorker")
        self._stop_event.clear()
        self._last_eviction_run = 0.0
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        logger.info("Stopping OutboxWorker")
        self._stop_event.set()
        if self._task:
            await self._task

    async def _run_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            processed = 0
            try:
                processed = await self.processor.process_batch()
            except Exception:
                logger.exception("OutboxWorker encountered an error during processing")
            await self._maybe_run_eviction(loop.time())
            if processed == 0:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass

    async def _maybe_run_eviction(self, now: float) -> None:
        if self.retention_policy is None or self.eviction_interval is None:
            return

        if self._last_eviction_run and (now - self._last_eviction_run) < self.eviction_interval:
            return

        try:
            deleted = await self.processor.storage.evict_published_messages(self.retention_policy)
            logger.info("Evicted %s published outbox rows", deleted)
            self._last_eviction_run = now
        except Exception:
            logger.exception("OutboxWorker eviction failed")
