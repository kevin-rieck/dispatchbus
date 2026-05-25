from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from dispatchr import get_metadata
from dispatchr.messages import (
    CommandBase,
    EventBase,
    InvalidMessageError,
    MessageMetadata,
    RuntimeMessage,
    as_runtime_message,
    derive_child_metadata,
    new_root_metadata,
)


@dataclass(frozen=True)
class CreateUser(CommandBase):
    message_name = "user.create"

    name: str
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("CreateUser is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "CreateUser":
        return CreateUser(name=self.name, _metadata=metadata)


@dataclass(frozen=True)
class UserCreated(EventBase):
    message_name = "user.created"

    user_id: int
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("UserCreated is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "UserCreated":
        return UserCreated(user_id=self.user_id, _metadata=metadata)


@dataclass(frozen=True)
class PlainCreateUser(CommandBase):
    message_name = "user.create"
    name: str


@dataclass(frozen=True)
class PlainUserCreated(EventBase):
    message_name = "user.created"
    user_id: int


def test_new_root_metadata_defaults_correlation_to_message_id() -> None:
    metadata = new_root_metadata()

    assert metadata.correlation_id == metadata.message_id
    assert metadata.causation_id is None


def test_new_root_metadata_preserves_supplied_correlation_id() -> None:
    metadata = new_root_metadata(correlation_id="trace-123")

    assert metadata.correlation_id == "trace-123"
    assert metadata.causation_id is None


def test_derive_child_metadata_inherits_correlation_and_sets_causation() -> None:
    parent = MessageMetadata(
        message_id="parent-1",
        correlation_id="corr-1",
        causation_id=None,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )

    child = derive_child_metadata(parent)

    assert child.message_id != parent.message_id
    assert child.correlation_id == "corr-1"
    assert child.causation_id == "parent-1"


def test_with_metadata_returns_new_immutable_instance() -> None:
    command = CreateUser(name="ada")
    stamped = command.with_metadata(new_root_metadata())

    assert stamped is not command
    assert stamped.name == "ada"
    assert stamped.metadata.message_id


def test_unstamped_message_metadata_property_raises() -> None:
    with pytest.raises(InvalidMessageError, match="unstamped"):
        _ = CreateUser(name="ada").metadata


def test_get_metadata_raises_for_unwrapped_plain_message() -> None:
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(PlainCreateUser(name="ada"))


def test_as_runtime_message_wraps_plain_root_message() -> None:
    payload = PlainCreateUser(name="ada")

    wrapped = as_runtime_message(payload)

    assert isinstance(wrapped, RuntimeMessage)
    assert wrapped.payload is payload
    assert wrapped.message_type is PlainCreateUser
    assert wrapped.metadata.correlation_id == wrapped.metadata.message_id
    assert get_metadata(wrapped) == wrapped.metadata


def test_as_runtime_message_preserves_legacy_metadata() -> None:
    legacy = CreateUser(name="ada").with_metadata(
        new_root_metadata(
            correlation_id="trace-123",
            message_id="msg-123",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        )
    )

    wrapped = as_runtime_message(legacy)

    assert wrapped.payload is legacy
    assert wrapped.metadata.message_id == "msg-123"
    assert wrapped.metadata.correlation_id == "trace-123"


def test_as_runtime_message_derives_child_metadata() -> None:
    parent = MessageMetadata(
        message_id="parent-1",
        correlation_id="corr-1",
        causation_id=None,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )

    wrapped = as_runtime_message(PlainUserCreated(user_id=3), parent=parent)

    assert wrapped.metadata.correlation_id == "corr-1"
    assert wrapped.metadata.causation_id == "parent-1"


def test_get_metadata_from_plain_payload_still_raises_after_wrapping() -> None:
    payload = PlainCreateUser(name="ada")

    wrapped = as_runtime_message(payload)

    assert get_metadata(wrapped) == wrapped.metadata
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(payload)


def test_legacy_message_reports_is_stamped_and_get_metadata() -> None:
    legacy = CreateUser(name="ada").with_metadata(new_root_metadata())

    assert legacy.is_stamped is True
    assert get_metadata(legacy) == legacy.metadata


def test_as_runtime_message_preserves_legacy_payload_identity_and_metadata() -> None:
    legacy = CreateUser(name="ada").with_metadata(
        new_root_metadata(correlation_id="trace-123", message_id="msg-123")
    )

    wrapped = as_runtime_message(legacy)

    assert wrapped.payload is legacy
    assert wrapped.metadata.message_id == "msg-123"
    assert wrapped.metadata.correlation_id == "trace-123"
