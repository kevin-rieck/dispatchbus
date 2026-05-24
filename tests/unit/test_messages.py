from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from dispatchr.messages import (
    CommandBase,
    EventBase,
    InvalidMessageError,
    MessageMetadata,
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
