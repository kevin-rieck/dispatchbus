import inspect
from dataclasses import dataclass
from typing import Any, cast

import pytest

from dispatchbus.bus import MessageBus
from dispatchbus.exceptions import BusUsageError, HandlerRegistrationError, InvalidMessageError
from dispatchbus.messages import (
    CommandBase,
    EventBase,
    MessageMetadata,
    get_metadata,
    new_root_metadata,
)
from dispatchbus.runtime import EventConcurrency


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


@dataclass(frozen=True)
class PlainAddUser(CommandBase):
    message_name = "user.add"
    name: str


@dataclass(frozen=True)
class PlainUserAdded(EventBase):
    message_name = "user.added"
    user_id: int


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


def test_message_bus_accepts_max_dispatch_chain_length_annotation() -> None:
    annotation = (
        inspect.signature(MessageBus.__init__).parameters["max_dispatch_chain_length"].annotation
    )
    assert annotation == int | None


def test_message_bus_rejects_non_positive_max_dispatch_chain_length() -> None:
    with pytest.raises(ValueError, match="max_dispatch_chain_length"):
        MessageBus(max_dispatch_chain_length=0)
    with pytest.raises(ValueError, match="max_dispatch_chain_length"):
        MessageBus(max_dispatch_chain_length=-1)


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
async def test_send_accepts_legacy_unstamped_root_command() -> None:
    bus = MessageBus()
    bus.register_command_handler(AddUserCommand, lambda command: command.name)

    assert await bus.send(AddUserCommand(name="ada")) == "ada"


@pytest.mark.asyncio
async def test_dispatched_plain_payload_does_not_become_metadata_readable() -> None:
    bus = MessageBus()
    seen: list[PlainAddUser] = []

    async def handler(command: PlainAddUser) -> str:
        seen.append(command)
        return command.name.upper()

    bus.register_command_handler(PlainAddUser, handler)

    payload = PlainAddUser(name="ada")
    result = await bus.send(payload)

    assert result == "ADA"
    assert seen == [payload]
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(payload)


@pytest.mark.asyncio
async def test_publish_plain_root_event_delivers_payload() -> None:
    bus = MessageBus()
    seen: list[PlainUserAdded] = []

    async def handler(event: PlainUserAdded) -> None:
        seen.append(event)

    bus.register_event_handler(PlainUserAdded, handler)

    payload = PlainUserAdded(user_id=3)
    await bus.publish(payload)

    assert seen == [payload]
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(payload)


@pytest.mark.asyncio
async def test_async_context_manager() -> None:
    async with MessageBus() as bus:
        assert isinstance(bus, MessageBus)
        assert bus._lifecycle._state.name == "OPEN"
    assert bus._lifecycle._state.name == "CLOSED"


def test_sync_context_manager() -> None:
    with MessageBus() as bus:
        assert isinstance(bus, MessageBus)
        assert bus._lifecycle._state.name == "OPEN"
    assert bus._lifecycle._state.name == "CLOSED"


def test_custom_executor() -> None:
    from concurrent.futures import ThreadPoolExecutor

    executor = ThreadPoolExecutor(max_workers=2)
    bus = MessageBus(executor=executor)
    assert bus._runtime._executor is executor
    assert bus._runtime._owns_executor is False
    bus.close()
    assert bus._runtime._executor is executor
    executor.shutdown(wait=True)
