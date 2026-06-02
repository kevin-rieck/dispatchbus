class DispatchbusError(Exception):
    """Base package exception."""


class HandlerRegistrationError(DispatchbusError):
    """Raised when handler registration is invalid."""


class DuplicateCommandHandlerError(HandlerRegistrationError):
    """Raised when more than one command handler is registered."""


class NoCommandHandlerError(DispatchbusError):
    """Raised when a command is dispatched without a registered handler."""


class EventPublicationError(DispatchbusError):
    """Raised when one or more event handlers fail."""

    def __init__(self, failures: list[Exception]) -> None:
        super().__init__(f"{len(failures)} event handler(s) failed")
        self.failures = failures


class MaxDispatchChainLengthExceededError(DispatchbusError):
    """Raised when a nested dispatch would exceed the configured chain length."""

    def __init__(self, max_dispatch_chain_length: int, attempted_depth: int) -> None:
        super().__init__(
            "max dispatch chain length "
            f"{max_dispatch_chain_length} exceeded by attempted depth {attempted_depth}"
        )
        self.max_dispatch_chain_length = max_dispatch_chain_length
        self.attempted_depth = attempted_depth


class BusDrainingError(DispatchbusError):
    """Raised when the message bus is draining and not accepting new messages."""


class BusUsageError(DispatchbusError):
    """Raised when the message bus API is used from an invalid runtime context."""


class InvalidMessageError(DispatchbusError):
    """Raised when a message violates the dispatchbus message contract."""
