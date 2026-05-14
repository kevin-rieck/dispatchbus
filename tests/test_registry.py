from dataclasses import dataclass

import pytest

from dispatchr.exceptions import DuplicateCommandHandlerError, NoCommandHandlerError
from dispatchr.registry import HandlerRegistry


@dataclass(frozen=True)
class CreateUser:
    name: str


@dataclass(frozen=True)
class UserCreated:
    user_id: int


async def async_create_user(command: CreateUser) -> str:
    return command.name.upper()


def sync_create_user(command: CreateUser) -> str:
    return command.name.lower()


async def on_user_created(event: UserCreated) -> None:
    return None


def test_register_and_resolve_command_handler() -> None:
    registry = HandlerRegistry()

    registry.register_command_handler(CreateUser, async_create_user)

    assert registry.get_command_handler(CreateUser) is async_create_user


def test_duplicate_command_handler_raises() -> None:
    registry = HandlerRegistry()
    registry.register_command_handler(CreateUser, async_create_user)

    with pytest.raises(DuplicateCommandHandlerError):
        registry.register_command_handler(CreateUser, sync_create_user)


def test_missing_command_handler_raises() -> None:
    registry = HandlerRegistry()

    with pytest.raises(NoCommandHandlerError):
        registry.get_command_handler(CreateUser)


def test_register_and_resolve_event_handlers() -> None:
    registry = HandlerRegistry()

    registry.register_event_handler(UserCreated, on_user_created)
    registry.register_event_handler(UserCreated, sync_create_user)

    handlers = registry.get_event_handlers(UserCreated)

    assert handlers == [on_user_created, sync_create_user]


def test_missing_event_handlers_returns_empty_list() -> None:
    registry = HandlerRegistry()

    assert registry.get_event_handlers(UserCreated) == []
