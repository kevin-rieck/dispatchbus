from dataclasses import dataclass

import pytest

from dispatchr.command_dispatch import CommandDispatcher
from dispatchr.exceptions import InvalidMessageError
from dispatchr.messages import (
    CommandBase,
    EventBase,
    MessageMetadata,
    RuntimeMessage,
    as_runtime_message,
    get_metadata,
    new_root_metadata,
)
from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import MessageRuntime


@dataclass(frozen=True)
class AddUser(CommandBase):
    message_name = "user.add"

    name: str
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("AddUser is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "AddUser":
        return AddUser(name=self.name, _metadata=metadata)


@dataclass(frozen=True)
class UserAdded(EventBase):
    message_name = "user.added"

    user_id: int
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("UserAdded is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "UserAdded":
        return UserAdded(user_id=self.user_id, _metadata=metadata)


@dataclass(frozen=True)
class PlainAddUser(CommandBase):
    message_name = "user.add"
    name: str


@dataclass(frozen=True)
class PlainUserAdded(EventBase):
    message_name = "user.added"
    user_id: int


@pytest.mark.asyncio
async def test_command_dispatcher_publishes_emitted_events() -> None:
    registry = HandlerRegistry()
    runtime = MessageRuntime(event_concurrency="sequential")
    seen: list[int] = []

    async def publish_event(event: object) -> None:
        if isinstance(event, RuntimeMessage) and isinstance(event.payload, UserAdded):
            seen.append(event.payload.user_id)

    dispatcher = CommandDispatcher(
        registry=registry,
        runtime=runtime,
        middleware=[],
        subscribers=[],
        publish_event=publish_event,
    )

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=len(command.name)))
        return command.name.upper()

    registry.register_command_handler(AddUser, command_handler)

    assert await dispatcher.send(AddUser(name="ada", _metadata=new_root_metadata())) == "ADA"
    assert seen == [3]


@pytest.mark.asyncio
async def test_command_dispatcher_stamps_emitted_events_from_parent_metadata() -> None:
    registry = HandlerRegistry()
    runtime = MessageRuntime(event_concurrency="sequential")
    seen: list[RuntimeMessage] = []

    async def publish_event(event: object) -> None:
        assert isinstance(event, RuntimeMessage)
        seen.append(event)

    dispatcher = CommandDispatcher(
        registry=registry,
        runtime=runtime,
        middleware=[],
        subscribers=[],
        publish_event=publish_event,
    )

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=7))
        return command.name.upper()

    registry.register_command_handler(AddUser, command_handler)

    command = AddUser(name="ada", _metadata=new_root_metadata(correlation_id="corr-77"))
    await dispatcher.send(command)

    emitted = seen[0]
    assert get_metadata(emitted).correlation_id == "corr-77"
    assert get_metadata(emitted).causation_id == command.metadata.message_id
    assert get_metadata(emitted).message_id != command.metadata.message_id


@pytest.mark.asyncio
async def test_command_dispatcher_stamps_plain_emitted_events_from_parent_metadata() -> None:
    registry = HandlerRegistry()
    runtime = MessageRuntime(event_concurrency="sequential")
    seen: list[RuntimeMessage] = []

    async def publish_event(event: object) -> None:
        assert isinstance(event, RuntimeMessage)
        seen.append(event)

    dispatcher = CommandDispatcher(
        registry=registry,
        runtime=runtime,
        middleware=[],
        subscribers=[],
        publish_event=publish_event,
    )

    async def command_handler(command: PlainAddUser, context) -> str:
        context.emit(PlainUserAdded(user_id=3))
        return command.name.upper()

    registry.register_command_handler(PlainAddUser, command_handler)

    command = as_runtime_message(PlainAddUser(name="ada"))
    await dispatcher.send(command)

    emitted = seen[0]
    assert emitted.payload == PlainUserAdded(user_id=3)
    assert get_metadata(emitted).correlation_id == get_metadata(command).correlation_id
    assert get_metadata(emitted).causation_id == get_metadata(command).message_id


def test_as_runtime_message_preserves_existing_runtime_message_wrapper() -> None:
    wrapped = as_runtime_message(PlainUserAdded(user_id=3))

    assert as_runtime_message(wrapped) is wrapped
