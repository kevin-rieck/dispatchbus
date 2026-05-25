from abc import ABC
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


_PAYLOAD_METADATA: dict[int, MessageMetadata] = {}


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


@dataclass(frozen=True)
class RuntimeMessage:
    payload: object
    metadata: MessageMetadata

    @property
    def message_type(self) -> type[object]:
        return type(self.payload)


def get_metadata(message: object) -> MessageMetadata:
    if isinstance(message, RuntimeMessage):
        return message.metadata

    try:
        metadata = message.metadata  # type: ignore[attr-defined]
    except Exception:
        metadata = None

    if isinstance(metadata, MessageMetadata):
        return metadata

    runtime_metadata = _PAYLOAD_METADATA.get(id(message))
    if runtime_metadata is not None:
        return runtime_metadata

    raise ValueError("Message metadata is unavailable for this object")


def as_runtime_message(
    message: object,
    parent: MessageMetadata | None = None,
) -> RuntimeMessage:
    if isinstance(message, RuntimeMessage):
        return message

    try:
        metadata = get_metadata(message)
    except ValueError:
        metadata = derive_child_metadata(parent) if parent is not None else new_root_metadata()

    _PAYLOAD_METADATA[id(message)] = metadata
    return RuntimeMessage(payload=message, metadata=metadata)


def payload_of(message: object) -> object:
    if isinstance(message, RuntimeMessage):
        return message.payload
    return message


def message_type_of(message: object) -> type[object]:
    return type(payload_of(message))


class MessageBase(ABC):
    message_name: ClassVar[str]
    schema_version: ClassVar[int] = 1

    @property
    def metadata(self) -> MessageMetadata:
        raise NotImplementedError

    def with_metadata(self, metadata: MessageMetadata) -> Self:
        raise NotImplementedError

    @property
    def is_stamped(self) -> bool:
        try:
            get_metadata(self)
        except (InvalidMessageError, ValueError):
            return False
        return True


class CommandBase(MessageBase):
    """Base class for messages dispatched via send()."""


class EventBase(MessageBase):
    """Base class for messages dispatched via publish()."""
