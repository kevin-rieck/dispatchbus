"""Dispatchr package."""

from dispatchr.bus import MessageBus
from dispatchr.exceptions import (
    DispatchrError,
    DuplicateCommandHandlerError,
    EventPublicationError,
    HandlerRegistrationError,
    NoCommandHandlerError,
)
from dispatchr.observability import (
    DispatchFinished,
    DispatchStarted,
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
)

__all__ = [
    "DispatchFinished",
    "DispatchStarted",
    "DispatchrError",
    "DuplicateCommandHandlerError",
    "EventPublicationError",
    "HandlerFailed",
    "HandlerFinished",
    "HandlerRegistrationError",
    "HandlerStarted",
    "MessageBus",
    "NoCommandHandlerError",
]
