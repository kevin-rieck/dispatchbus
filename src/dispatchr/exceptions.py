class DispatchrError(Exception):
    """Base package exception."""


class HandlerRegistrationError(DispatchrError):
    """Raised when handler registration is invalid."""


class DuplicateCommandHandlerError(HandlerRegistrationError):
    """Raised when more than one command handler is registered."""


class NoCommandHandlerError(DispatchrError):
    """Raised when a command is dispatched without a registered handler."""
