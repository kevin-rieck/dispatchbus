"""Dispatchr package."""

from dispatchr.bus import MessageBus
from dispatchr.exceptions import (
    BusUsageError,
    DispatchrError,
    DuplicateCommandHandlerError,
    EventPublicationError,
    HandlerRegistrationError,
    NoCommandHandlerError,
)
from dispatchr.messages import (
    CommandBase,
    EventBase,
    MessageBase,
    MessageMetadata,
    derive_child_metadata,
    new_root_metadata,
)
from dispatchr.observability import (
    DispatchFinished,
    DispatchStarted,
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
)

__all__ = [
    "CommandBase",
    "DispatchFinished",
    "DispatchStarted",
    "BusUsageError",
    "DispatchrError",
    "DuplicateCommandHandlerError",
    "EventBase",
    "EventPublicationError",
    "HandlerFailed",
    "HandlerFinished",
    "HandlerRegistrationError",
    "HandlerStarted",
    "MessageBase",
    "MessageBus",
    "MessageMetadata",
    "NoCommandHandlerError",
    "derive_child_metadata",
    "new_root_metadata",
]
