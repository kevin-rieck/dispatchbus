from dataclasses import dataclass
from datetime import datetime
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


class OutboxStorage(Protocol):
    async def claim_pending_messages(self, batch_size: int) -> list[OutboxMessage]: ...

    async def mark_as_published(self, message_ids: list[str]) -> None: ...

    async def release_claims(self, message_ids: list[str]) -> None: ...


class MessageSerializer(Protocol):
    def serialize(self, message: MessageBase) -> bytes: ...

    def deserialize(self, message_type: str, payload: bytes) -> MessageBase: ...


class EventPublisher(Protocol):
    async def publish(self, event: MessageBase) -> None: ...
