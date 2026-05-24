from dataclasses import dataclass

import pytest

from dispatchr.exceptions import (
    DuplicateCommandHandlerError,
    HandlerRegistrationError,
    InvalidMessageError,
    NoCommandHandlerError,
)
from dispatchr.messages import CommandBase, EventBase, MessageMetadata
from dispatchr.registry import HandlerRegistry


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


async def async_create_user(command: CreateUser) -> str:
    return command.name.upper()


def sync_create_user(command: CreateUser) -> str:
    return command.name.lower()


async def on_user_created(event: UserCreated) -> None:
    return None


def test_register_and_resolve_command_handler() -> None:
    registry = HandlerRegistry()

    registry.register_command_handler(CreateUser, async_create_user)

    registered_handler = registry.get_command_handler(CreateUser)

    assert registered_handler.handler is async_create_user
    assert registered_handler.is_async is True
    assert registered_handler.context_style == "none"


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

    assert [handler.handler for handler in handlers] == [on_user_created, sync_create_user]
    assert [handler.is_async for handler in handlers] == [True, False]
    assert [handler.context_style for handler in handlers] == ["none", "none"]


def test_registers_keyword_only_context_metadata() -> None:
    registry = HandlerRegistry()

    async def handler(event: UserCreated, *, context) -> None:
        return None

    registry.register_event_handler(UserCreated, handler)

    registered_handler = registry.get_event_handlers(UserCreated)[0]

    assert registered_handler.handler is handler
    assert registered_handler.context_style == "keyword"


def test_missing_event_handlers_returns_empty_list() -> None:
    registry = HandlerRegistry()

    assert registry.get_event_handlers(UserCreated) == []


def test_register_command_handler_rejects_non_command_type() -> None:
    registry = HandlerRegistry()

    with pytest.raises(HandlerRegistrationError, match="CommandBase"):
        registry.register_command_handler(UserCreated, async_create_user)


def test_register_event_handler_rejects_non_event_type() -> None:
    registry = HandlerRegistry()

    with pytest.raises(HandlerRegistrationError, match="EventBase"):
        registry.register_event_handler(CreateUser, on_user_created)
