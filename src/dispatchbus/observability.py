from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import uuid4

from dispatchbus.messages import MessageMetadata

Operation = Literal["send", "publish"]
Subscriber = Callable[[Any], Any]


def new_dispatch_id() -> str:
    return uuid4().hex


def handler_name(handler: Callable[[Any], Any]) -> str:
    module = getattr(handler, "__module__", handler.__class__.__module__)
    qualname = getattr(handler, "__qualname__", handler.__class__.__qualname__)
    return f"{module}.{qualname}"


@dataclass(frozen=True)
class DispatchStarted:
    message: Any
    metadata: MessageMetadata
    message_type: type[Any]
    operation: Operation
    timestamp: datetime
    dispatch_id: str
    handler_count: int


@dataclass(frozen=True)
class DispatchFinished:
    message: Any
    metadata: MessageMetadata
    message_type: type[Any]
    operation: Operation
    timestamp: datetime
    dispatch_id: str
    handler_count: int
    duration_ms: float
    success: bool


@dataclass(frozen=True)
class HandlerStarted:
    message: Any
    metadata: MessageMetadata
    message_type: type[Any]
    operation: Operation
    timestamp: datetime
    dispatch_id: str
    handler: Callable[[Any], Any]
    handler_name: str


@dataclass(frozen=True)
class HandlerFinished:
    message: Any
    metadata: MessageMetadata
    message_type: type[Any]
    operation: Operation
    timestamp: datetime
    dispatch_id: str
    handler: Callable[[Any], Any]
    handler_name: str
    duration_ms: float


@dataclass(frozen=True)
class HandlerFailed:
    message: Any
    metadata: MessageMetadata
    message_type: type[Any]
    operation: Operation
    timestamp: datetime
    dispatch_id: str
    handler: Callable[[Any], Any]
    handler_name: str
    duration_ms: float
    error: Exception
