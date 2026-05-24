from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar, Self
from uuid import uuid4

from dispatchr.exceptions import InvalidMessageError


@dataclass(frozen=True)
class MessageMetadata:
    message_id: str
    correlation_id: str
    causation_id: str | None
    timestamp: datetime


def new_root_metadata(
    *,
    correlation_id: str | None = None,
    message_id: str | None = None,
    timestamp: datetime | None = None,
) -> MessageMetadata:
    resolved_message_id = message_id or uuid4().hex
    resolved_timestamp = timestamp or datetime.now(UTC)
    return MessageMetadata(
        message_id=resolved_message_id,
        correlation_id=correlation_id or resolved_message_id,
        causation_id=None,
        timestamp=resolved_timestamp,
    )


def derive_child_metadata(
    parent: MessageMetadata,
    *,
    message_id: str | None = None,
    timestamp: datetime | None = None,
) -> MessageMetadata:
    return MessageMetadata(
        message_id=message_id or uuid4().hex,
        correlation_id=parent.correlation_id,
        causation_id=parent.message_id,
        timestamp=timestamp or datetime.now(UTC),
    )


class MessageBase(ABC):
    message_name: ClassVar[str]
    schema_version: ClassVar[int] = 1

    @property
    @abstractmethod
    def metadata(self) -> MessageMetadata:
        raise NotImplementedError

    @abstractmethod
    def with_metadata(self, metadata: MessageMetadata) -> Self:
        raise NotImplementedError

    @property
    def is_stamped(self) -> bool:
        try:
            self.metadata
        except InvalidMessageError:
            return False
        return True


class CommandBase(MessageBase):
    """Base class for messages dispatched via send()."""


class EventBase(MessageBase):
    """Base class for messages dispatched via publish()."""
