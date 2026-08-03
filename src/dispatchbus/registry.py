import inspect
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from dispatchbus.callable_runtime import is_async_callable
from dispatchbus.exceptions import (
    DuplicateCommandHandlerError,
    HandlerRegistrationError,
    NoCommandHandlerError,
)
from dispatchbus.messages import CommandBase, EventBase

Handler = Callable[..., Any]
ContextStyle = Literal["none", "positional", "keyword"]


@dataclass(frozen=True)
class RegisteredHandler:
    handler: Callable[..., Any]
    is_async: bool
    context_style: ContextStyle


def _context_style(value: Callable[..., Any]) -> ContextStyle:
    parameters = list(inspect.signature(value).parameters.values())
    positional = [
        parameter
        for parameter in parameters
        if parameter.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    if (
        len(positional) >= 2
        and positional[1].name == "context"
        and (positional[1].default is inspect.Parameter.empty or positional[1].default is None)
    ):
        return "positional"
    for parameter in parameters:
        if (
            parameter.kind is inspect.Parameter.KEYWORD_ONLY
            and parameter.name == "context"
            and (parameter.default is inspect.Parameter.empty or parameter.default is None)
        ):
            return "keyword"
    return "none"


def _register_handler(handler: Callable[..., Any]) -> RegisteredHandler:
    return RegisteredHandler(
        handler=handler,
        is_async=is_async_callable(handler),
        context_style=_context_style(handler),
    )


def _require_command_type(message_type: type[Any]) -> None:
    if not issubclass(message_type, CommandBase):
        raise HandlerRegistrationError(
            f"command handlers require a CommandBase subclass, got {message_type.__name__}"
        )


def _require_event_type(message_type: type[Any]) -> None:
    if not issubclass(message_type, EventBase):
        raise HandlerRegistrationError(
            f"event handlers require an EventBase subclass, got {message_type.__name__}"
        )


class HandlerRegistry:
    def __init__(self) -> None:
        self._command_handlers: dict[type[Any], RegisteredHandler] = {}
        self._event_handlers: dict[type[Any], list[RegisteredHandler]] = defaultdict(list)

    def register_command_handler(self, message_type: type[Any], handler: Handler) -> None:
        _require_command_type(message_type)
        if message_type in self._command_handlers:
            raise DuplicateCommandHandlerError(
                f"command handler already registered for {message_type.__name__}"
            )
        self._command_handlers[message_type] = _register_handler(handler)

    def register_event_handler(self, message_type: type[Any], handler: Handler) -> None:
        _require_event_type(message_type)
        self._event_handlers[message_type].append(_register_handler(handler))

    def get_command_handler(self, message_type: type[Any]) -> RegisteredHandler:
        try:
            return self._command_handlers[message_type]
        except KeyError as exc:
            raise NoCommandHandlerError(
                f"no command handler registered for {message_type.__name__}"
            ) from exc

    def get_event_handlers(self, message_type: type[Any]) -> list[RegisteredHandler]:
        return list(self._event_handlers.get(message_type, []))
