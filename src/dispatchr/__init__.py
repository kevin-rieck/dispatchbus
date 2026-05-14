"""Dispatchr package."""

from dispatchr.bus import MessageBus
from dispatchr.exceptions import (
    DispatchrError,
    DuplicateCommandHandlerError,
    EventPublicationError,
    HandlerRegistrationError,
    NoCommandHandlerError,
)

__all__ = [
    "DispatchrError",
    "DuplicateCommandHandlerError",
    "EventPublicationError",
    "HandlerRegistrationError",
    "MessageBus",
    "NoCommandHandlerError",
]
