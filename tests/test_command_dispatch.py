from dataclasses import dataclass

import pytest

from dispatchr.command_dispatch import CommandDispatcher
from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import MessageRuntime


@dataclass(frozen=True)
class AddUser:
    name: str


@dataclass(frozen=True)
class UserAdded:
    user_id: int


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

    assert await dispatcher.send(AddUser(name="ada")) == "ADA"
    assert seen == [3]
