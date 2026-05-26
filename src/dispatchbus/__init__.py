"""Dispatchbus package."""

from dispatchbus.bus import MessageBus
from dispatchbus.exceptions import (
    BusUsageError,
    DispatchbusError,
    DuplicateCommandHandlerError,
    EventPublicationError,
    HandlerRegistrationError,
    NoCommandHandlerError,
)
from dispatchbus.messages import (
    CommandBase,
    EventBase,
    MessageBase,
    MessageMetadata,
    derive_child_metadata,
    get_metadata,
    new_root_metadata,
)
from dispatchbus.observability import (
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
    "DispatchbusError",
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
    "get_metadata",
    "new_root_metadata",
]
