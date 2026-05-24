from dataclasses import dataclass

import pytest

from dispatchr.command_dispatch import CommandDispatcher
from dispatchr.exceptions import InvalidMessageError
from dispatchr.messages import CommandBase, EventBase, MessageMetadata, new_root_metadata
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


@pytest.mark.asyncio
async def test_command_dispatcher_publishes_emitted_events() -> None:
    registry = HandlerRegistry()
    runtime = MessageRuntime(event_concurrency="sequential")
    seen: list[int] = []

    async def publish_event(event: object) -> None:
        if isinstance(event, UserAdded):
            seen.append(event.user_id)

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
    seen: list[UserAdded] = []

    async def publish_event(event: object) -> None:
        assert isinstance(event, UserAdded)
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
    assert emitted.metadata.correlation_id == "corr-77"
    assert emitted.metadata.causation_id == command.metadata.message_id
    assert emitted.metadata.message_id != command.metadata.message_id
