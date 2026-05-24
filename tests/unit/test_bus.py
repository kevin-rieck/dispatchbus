import inspect
from typing import Any, cast

import pytest

from dispatchr.bus import MessageBus
from dispatchr.exceptions import HandlerRegistrationError
from dispatchr.runtime import EventConcurrency


def test_message_bus_uses_lifecycle_collaborator() -> None:
    bus = MessageBus()

    assert bus._lifecycle is not None


def test_message_bus_uses_sync_bridge() -> None:
    bus = MessageBus()

    assert bus._sync_bridge is not None


def test_message_bus_uses_event_publisher() -> None:
    bus = MessageBus()

    assert bus._event_publisher is not None


def test_message_bus_uses_command_dispatcher() -> None:
    bus = MessageBus()

    assert bus._command_dispatcher is not None


def test_message_bus_event_concurrency_annotation_matches_runtime_type() -> None:
    annotation = inspect.signature(MessageBus.__init__).parameters["event_concurrency"].annotation

    assert annotation == EventConcurrency


def test_invalid_event_concurrency_raises() -> None:
    with pytest.raises(HandlerRegistrationError):
        MessageBus(event_concurrency=cast(Any, "bogus"))
