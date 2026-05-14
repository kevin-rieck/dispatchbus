from collections import defaultdict
from collections.abc import Callable
from typing import Any

from dispatchr.exceptions import DuplicateCommandHandlerError, NoCommandHandlerError

Handler = Callable[[Any], Any]


class HandlerRegistry:
    def __init__(self) -> None:
        self._command_handlers: dict[type[Any], Handler] = {}
        self._event_handlers: dict[type[Any], list[Handler]] = defaultdict(list)

    def register_command_handler(self, message_type: type[Any], handler: Handler) -> None:
        if message_type in self._command_handlers:
            raise DuplicateCommandHandlerError(
                f"command handler already registered for {message_type.__name__}"
            )
        self._command_handlers[message_type] = handler

    def register_event_handler(self, message_type: type[Any], handler: Handler) -> None:
        self._event_handlers[message_type].append(handler)

    def get_command_handler(self, message_type: type[Any]) -> Handler:
        try:
            return self._command_handlers[message_type]
        except KeyError as exc:
            raise NoCommandHandlerError(
                f"no command handler registered for {message_type.__name__}"
            ) from exc

    def get_event_handlers(self, message_type: type[Any]) -> list[Handler]:
        return list(self._event_handlers.get(message_type, []))
