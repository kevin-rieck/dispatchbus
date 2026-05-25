from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from dispatchr import MessageBase

from .serializers import JSONSerializer
from .processor import OutboxProcessor, OutboxWorker


@dataclass
class OutboxMessage:
    id: str
    message_type: str
    payload: bytes
    created_at: datetime
    published_at: datetime | None = None


class OutboxStorage(Protocol):
    async def get_pending_messages(self, batch_size: int) -> list[OutboxMessage]: ...

    async def mark_as_published(self, message_ids: list[str]) -> None: ...


class MessageSerializer(Protocol):
    def serialize(self, message: MessageBase) -> bytes: ...

    def deserialize(self, message_type: str, payload: bytes) -> MessageBase: ...


class EventPublisher(Protocol):
    async def publish(self, event: MessageBase) -> None: ...


__all__ = [
    "OutboxMessage",
    "OutboxStorage",
    "MessageSerializer",
    "EventPublisher",
    "JSONSerializer",
    "OutboxProcessor",
    "OutboxWorker",
]
