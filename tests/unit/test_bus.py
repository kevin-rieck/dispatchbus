import inspect
from dataclasses import dataclass
from typing import Any, cast

import pytest

from dispatchr.bus import MessageBus
from dispatchr.exceptions import BusUsageError, HandlerRegistrationError, InvalidMessageError
from dispatchr.messages import CommandBase, EventBase, MessageMetadata, new_root_metadata
from dispatchr.runtime import EventConcurrency


@dataclass(frozen=True)
class AddUserCommand(CommandBase):
    message_name = "user.add"

    name: str
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("AddUserCommand is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "AddUserCommand":
        return AddUserCommand(name=self.name, _metadata=metadata)


@dataclass(frozen=True)
class UserAddedEvent(EventBase):
    message_name = "user.added"

    user_id: int
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("UserAddedEvent is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "UserAddedEvent":
        return UserAddedEvent(user_id=self.user_id, _metadata=metadata)


def test_message_bus_uses_lifecycle_collaborator() -> None:
    bus = MessageBus()

    assert bus._lifecycle is not None


def test_message_bus_uses_sync_bridge() -> None:
    bus = MessageBus()

    assert bus._sync_bridge is not None


def test_message_bus_uses_event_publisher() -> None:
    bus = MessageBus()

    assert bus._event_publisher is not None


def test_message_bus_uses_command_dispatcher() -> None:
    bus = MessageBus()

    assert bus._command_dispatcher is not None


def test_message_bus_event_concurrency_annotation_matches_runtime_type() -> None:
    annotation = inspect.signature(MessageBus.__init__).parameters["event_concurrency"].annotation

    assert annotation == EventConcurrency


def test_invalid_event_concurrency_raises() -> None:
    with pytest.raises(HandlerRegistrationError):
        MessageBus(event_concurrency=cast(Any, "bogus"))


@pytest.mark.asyncio
async def test_send_rejects_event_instances() -> None:
    bus = MessageBus()

    with pytest.raises(BusUsageError, match="CommandBase"):
        await bus.send(UserAddedEvent(user_id=1, _metadata=new_root_metadata()))


@pytest.mark.asyncio
async def test_publish_rejects_command_instances() -> None:
    bus = MessageBus()

    with pytest.raises(BusUsageError, match="EventBase"):
        await bus.publish(AddUserCommand(name="ada", _metadata=new_root_metadata()))


@pytest.mark.asyncio
async def test_send_rejects_unstamped_root_command() -> None:
    bus = MessageBus()
    bus.register_command_handler(AddUserCommand, lambda command: command.name)

    with pytest.raises(BusUsageError, match="stamped"):
        await bus.send(AddUserCommand(name="ada"))
