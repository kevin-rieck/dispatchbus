from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from dispatchr.messages import new_root_metadata
from dispatchr.observability import (
    DispatchFinished,
    DispatchStarted,
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
    handler_name,
)


async def local_observability_handler(message: object) -> None:
    return None


def test_handler_name_uses_module_and_qualname() -> None:
    assert handler_name(local_observability_handler).endswith("local_observability_handler")


def test_lifecycle_events_are_frozen_dataclasses() -> None:
    message = object()
    now = datetime.now()
    error = ValueError("boom")
    metadata = new_root_metadata()

    started = DispatchStarted(
        message=message,
        metadata=metadata,
        message_type=object,
        operation="send",
        timestamp=now,
        dispatch_id="dispatch-1",
        handler_count=1,
    )
    finished = DispatchFinished(
        message=message,
        metadata=metadata,
        message_type=object,
        operation="send",
        timestamp=now,
        dispatch_id="dispatch-1",
        handler_count=1,
        duration_ms=1.25,
        success=True,
    )
    handler_started = HandlerStarted(
        message=message,
        metadata=metadata,
        message_type=object,
        operation="send",
        timestamp=now,
        dispatch_id="dispatch-1",
        handler=local_observability_handler,
        handler_name=handler_name(local_observability_handler),
    )
    handler_finished = HandlerFinished(
        message=message,
        metadata=metadata,
        message_type=object,
        operation="send",
        timestamp=now,
        dispatch_id="dispatch-1",
        handler=local_observability_handler,
        handler_name=handler_name(local_observability_handler),
        duration_ms=0.5,
    )
    handler_failed = HandlerFailed(
        message=message,
        metadata=metadata,
        message_type=object,
        operation="send",
        timestamp=now,
        dispatch_id="dispatch-1",
        handler=local_observability_handler,
        handler_name=handler_name(local_observability_handler),
        duration_ms=0.5,
        error=error,
    )

    assert started.handler_count == 1
    assert finished.success is True
    assert handler_started.handler is local_observability_handler
    assert handler_finished.duration_ms == 0.5
    assert handler_failed.error is error

    with pytest.raises(FrozenInstanceError):
        started.dispatch_id = "other"


def test_lifecycle_events_expose_message_metadata() -> None:
    payload = object()
    metadata = new_root_metadata(
        message_id="msg-1",
        correlation_id="corr-1",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )

    started = DispatchStarted(
        message=payload,
        metadata=metadata,
        message_type=object,
        operation="send",
        timestamp=datetime.now(),
        dispatch_id="dispatch-1",
        handler_count=1,
    )

    assert started.message is payload
    assert started.metadata.message_id == "msg-1"
