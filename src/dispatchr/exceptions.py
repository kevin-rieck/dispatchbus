class DispatchrError(Exception):
    """Base package exception."""


class HandlerRegistrationError(DispatchrError):
    """Raised when handler registration is invalid."""


class DuplicateCommandHandlerError(HandlerRegistrationError):
    """Raised when more than one command handler is registered."""


class NoCommandHandlerError(DispatchrError):
    """Raised when a command is dispatched without a registered handler."""


class EventPublicationError(DispatchrError):
    """Raised when one or more event handlers fail."""

    def __init__(self, failures: list[Exception]) -> None:
        super().__init__(f"{len(failures)} event handler(s) failed")
        self.failures = failures


class BusDrainingError(DispatchrError):
    """Raised when the message bus is draining and not accepting new messages."""
