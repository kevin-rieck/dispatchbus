from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from dispatchr import MessageBase


@dataclass
class OutboxMessage:
    id: str
    message_type: str
    payload: bytes
    created_at: datetime
    claimed_at: datetime | None = None
    published_at: datetime | None = None


@dataclass(frozen=True)
class OutboxRetentionPolicy:
    retention_period: timedelta
    max_published_rows: int | None = None

    def __post_init__(self) -> None:
        if self.retention_period < timedelta(0):
            raise ValueError("retention_period must be non-negative")
        if self.max_published_rows is not None and self.max_published_rows < 0:
            raise ValueError("max_published_rows must be non-negative")


class OutboxStorage(Protocol):
    async def claim_pending_messages(self, batch_size: int) -> list[OutboxMessage]: ...

    async def mark_as_published(self, message_ids: list[str]) -> None: ...

    async def release_claims(self, message_ids: list[str]) -> None: ...

    async def evict_published_messages(self, policy: OutboxRetentionPolicy) -> int: ...


class MessageSerializer(Protocol):
    def serialize(self, message: MessageBase) -> bytes: ...

    def deserialize(self, message_type: str, payload: bytes) -> MessageBase: ...


class EventPublisher(Protocol):
    async def publish(self, event: MessageBase) -> None: ...
