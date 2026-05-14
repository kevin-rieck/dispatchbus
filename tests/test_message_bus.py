import asyncio
import inspect
from dataclasses import FrozenInstanceError, dataclass
from datetime import datetime
from typing import Any, cast

import pytest

from dispatchr.bus import MessageBus
from dispatchr.exceptions import EventPublicationError, HandlerRegistrationError
from dispatchr.observability import (
    DispatchFinished,
    DispatchStarted,
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
    handler_name,
)
from dispatchr.runtime import EventConcurrency


@dataclass(frozen=True)
class AddUser:
    name: str


@dataclass(frozen=True)
class UserAdded:
    user_id: int


async def local_observability_handler(message: object) -> None:
    return None


def test_handler_name_uses_module_and_qualname() -> None:
    assert handler_name(local_observability_handler).endswith("local_observability_handler")


def test_lifecycle_events_are_frozen_dataclasses() -> None:
    message = object()
    now = datetime.now()
    error = ValueError("boom")

    started = DispatchStarted(
        message=message,
        message_type=object,
        operation="send",
        timestamp=now,
        dispatch_id="dispatch-1",
        handler_count=1,
    )
    finished = DispatchFinished(
        message=message,
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
        message_type=object,
        operation="send",
        timestamp=now,
        dispatch_id="dispatch-1",
        handler=local_observability_handler,
        handler_name=handler_name(local_observability_handler),
    )
    handler_finished = HandlerFinished(
        message=message,
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


@pytest.mark.asyncio
async def test_send_uses_async_command_handler() -> None:
    bus = MessageBus()

    async def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    result = await bus.send(AddUser(name="ada"))

    assert result == "ADA"


@pytest.mark.asyncio
async def test_send_uses_sync_command_handler() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name.lower()

    bus.register_command_handler(AddUser, handler)

    result = await bus.send(AddUser(name="ADA"))

    assert result == "ada"


@pytest.mark.asyncio
async def test_publish_fans_out_to_all_handlers() -> None:
    bus = MessageBus()
    seen: list[str] = []

    async def async_handler(event: UserAdded) -> None:
        seen.append(f"async:{event.user_id}")

    def sync_handler(event: UserAdded) -> None:
        seen.append(f"sync:{event.user_id}")

    bus.register_event_handler(UserAdded, async_handler)
    bus.register_event_handler(UserAdded, sync_handler)

    await bus.publish(UserAdded(user_id=7))

    assert sorted(seen) == ["async:7", "sync:7"]


@pytest.mark.asyncio
async def test_publish_raises_aggregate_error() -> None:
    bus = MessageBus()

    async def ok_handler(event: UserAdded) -> None:
        return None

    async def bad_handler(event: UserAdded) -> None:
        raise ValueError("boom")

    bus.register_event_handler(UserAdded, ok_handler)
    bus.register_event_handler(UserAdded, bad_handler)

    with pytest.raises(EventPublicationError) as exc_info:
        await bus.publish(UserAdded(user_id=3))

    assert len(exc_info.value.failures) == 1
    assert isinstance(exc_info.value.failures[0], ValueError)


@pytest.mark.asyncio
async def test_send_applies_middleware_in_order() -> None:
    events: list[str] = []

    async def first(message, call_next):
        events.append("first:before")
        result = await call_next(message)
        events.append("first:after")
        return result

    async def second(message, call_next):
        events.append("second:before")
        result = await call_next(message)
        events.append("second:after")
        return result

    bus = MessageBus(middleware=[first, second])

    async def handler(command: AddUser) -> str:
        events.append("handler")
        return command.name

    bus.register_command_handler(AddUser, handler)

    result = await bus.send(AddUser(name="ada"))

    assert result == "ada"
    assert events == [
        "first:before",
        "second:before",
        "handler",
        "second:after",
        "first:after",
    ]


@pytest.mark.asyncio
async def test_publish_applies_middleware_once_for_the_operation() -> None:
    events: list[str] = []

    async def middleware(message, call_next):
        events.append("before")
        await call_next(message)
        events.append("after")

    bus = MessageBus(middleware=[middleware])

    async def handler(event: UserAdded) -> None:
        events.append(f"handler:{event.user_id}")

    bus.register_event_handler(UserAdded, handler)

    await bus.publish(UserAdded(user_id=9))

    assert events == ["before", "handler:9", "after"]


@pytest.mark.asyncio
async def test_publish_with_no_subscribers_is_a_no_op() -> None:
    bus = MessageBus()

    await bus.publish(UserAdded(user_id=11))


@pytest.mark.asyncio
async def test_subscriber_failures_are_ignored() -> None:
    seen: list[str] = []

    async def bad_subscriber(event: object) -> None:
        raise RuntimeError("subscriber boom")

    async def good_subscriber(event: object) -> None:
        seen.append(type(event).__name__)

    bus = MessageBus(subscribers=[bad_subscriber, good_subscriber])

    async def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    result = await bus.send(AddUser(name="ada"))

    assert result == "ADA"
    assert seen == ["DispatchStarted", "HandlerStarted", "HandlerFinished", "DispatchFinished"]


@pytest.mark.asyncio
async def test_send_emits_lifecycle_events_in_order() -> None:
    seen: list[object] = []

    async def subscriber(event: object) -> None:
        seen.append(event)

    bus = MessageBus(subscribers=[subscriber])

    async def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    result = await bus.send(AddUser(name="ada"))

    assert result == "ADA"
    assert [type(event) for event in seen] == [
        DispatchStarted,
        HandlerStarted,
        HandlerFinished,
        DispatchFinished,
    ]
    assert all(event.operation == "send" for event in seen)
    dispatch_ids = {event.dispatch_id for event in seen}
    assert len(dispatch_ids) == 1
    assert seen[0].handler_count == 1
    assert seen[3].success is True


@pytest.mark.asyncio
async def test_publish_emits_events_for_each_handler() -> None:
    seen: list[object] = []

    async def subscriber(event: object) -> None:
        seen.append(event)

    bus = MessageBus(subscribers=[subscriber], event_concurrency="sequential")

    async def first(event: UserAdded) -> None:
        return None

    async def second(event: UserAdded) -> None:
        return None

    bus.register_event_handler(UserAdded, first)
    bus.register_event_handler(UserAdded, second)

    await bus.publish(UserAdded(user_id=7))

    assert [type(event) for event in seen] == [
        DispatchStarted,
        HandlerStarted,
        HandlerFinished,
        HandlerStarted,
        HandlerFinished,
        DispatchFinished,
    ]
    assert seen[0].handler_count == 2
    assert seen[-1].success is True


@pytest.mark.asyncio
async def test_publish_failure_emits_handler_failed_and_unsuccessful_dispatch_finished() -> None:
    seen: list[object] = []

    async def subscriber(event: object) -> None:
        seen.append(event)

    bus = MessageBus(subscribers=[subscriber], event_concurrency="sequential")

    async def ok_handler(event: UserAdded) -> None:
        return None

    async def bad_handler(event: UserAdded) -> None:
        raise ValueError("boom")

    bus.register_event_handler(UserAdded, ok_handler)
    bus.register_event_handler(UserAdded, bad_handler)

    with pytest.raises(EventPublicationError):
        await bus.publish(UserAdded(user_id=8))

    assert [type(event) for event in seen] == [
        DispatchStarted,
        HandlerStarted,
        HandlerFinished,
        HandlerStarted,
        HandlerFailed,
        DispatchFinished,
    ]
    assert isinstance(seen[-2], HandlerFailed)
    assert seen[-2].error.args == ("boom",)
    assert isinstance(seen[-1], DispatchFinished)
    assert seen[-1].success is False


@pytest.mark.asyncio
async def test_publish_concurrent_events_keep_per_handler_order() -> None:
    seen: list[tuple[str, str]] = []
    first_started = asyncio.Event()
    release = asyncio.Event()

    async def subscriber(event: object) -> None:
        if hasattr(event, "handler_name"):
            seen.append((type(event).__name__, event.handler_name))

    bus = MessageBus(subscribers=[subscriber], event_concurrency="concurrent")

    async def first(event: UserAdded) -> None:
        first_started.set()
        await release.wait()

    async def second(event: UserAdded) -> None:
        await first_started.wait()
        release.set()

    bus.register_event_handler(UserAdded, first)
    bus.register_event_handler(UserAdded, second)

    await bus.publish(UserAdded(user_id=9))

    grouped: dict[str, list[str]] = {}
    for event_name, name in seen:
        grouped.setdefault(name, []).append(event_name)

    assert len(grouped) == 2
    assert all(names == ["HandlerStarted", "HandlerFinished"] for names in grouped.values())


@pytest.mark.asyncio
async def test_sync_subscriber_receives_lifecycle_events() -> None:
    seen: list[str] = []

    def subscriber(event: object) -> None:
        seen.append(type(event).__name__)

    bus = MessageBus(subscribers=[subscriber])

    async def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    result = await bus.send(AddUser(name="ada"))

    assert result == "ADA"
    assert seen == ["DispatchStarted", "HandlerStarted", "HandlerFinished", "DispatchFinished"]


def test_send_sync_runs_command_through_background_runtime() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return f"sync:{command.name}"

    bus.register_command_handler(AddUser, handler)

    result = bus.send_sync(AddUser(name="ada"))

    assert result == "sync:ada"
    bus.close()


def test_publish_sync_runs_event_handlers_through_background_runtime() -> None:
    bus = MessageBus()
    seen: list[str] = []

    def handler(event: UserAdded) -> None:
        seen.append(f"event:{event.user_id}")

    bus.register_event_handler(UserAdded, handler)

    bus.publish_sync(UserAdded(user_id=21))

    assert seen == ["event:21"]
    bus.close()


@pytest.mark.asyncio
async def test_publish_can_run_handlers_sequentially() -> None:
    bus = MessageBus(event_concurrency="sequential")
    seen: list[str] = []

    async def first(event: UserAdded) -> None:
        seen.append("first")

    async def second(event: UserAdded) -> None:
        seen.append("second")

    bus.register_event_handler(UserAdded, first)
    bus.register_event_handler(UserAdded, second)

    await bus.publish(UserAdded(user_id=1))

    assert seen == ["first", "second"]


@pytest.mark.asyncio
async def test_publish_can_run_handlers_concurrently() -> None:
    bus = MessageBus(event_concurrency="concurrent")
    started = asyncio.Event()
    release = asyncio.Event()
    seen: list[str] = []

    async def first(event: UserAdded) -> None:
        seen.append("first:start")
        started.set()
        await release.wait()
        seen.append("first:end")

    async def second(event: UserAdded) -> None:
        await started.wait()
        seen.append("second:start")
        release.set()
        seen.append("second:end")

    bus.register_event_handler(UserAdded, first)
    bus.register_event_handler(UserAdded, second)

    await bus.publish(UserAdded(user_id=2))

    assert seen == ["first:start", "second:start", "second:end", "first:end"]


def test_public_api_exports_message_bus() -> None:
    from dispatchr import MessageBus

    assert MessageBus.__name__ == "MessageBus"


def test_public_api_exports_observability_events() -> None:
    from dispatchr import (
        DispatchFinished,
        DispatchStarted,
        HandlerFailed,
        HandlerFinished,
        HandlerStarted,
    )

    assert DispatchStarted.__name__ == "DispatchStarted"
    assert DispatchFinished.__name__ == "DispatchFinished"
    assert HandlerStarted.__name__ == "HandlerStarted"
    assert HandlerFinished.__name__ == "HandlerFinished"
    assert HandlerFailed.__name__ == "HandlerFailed"


def test_message_bus_event_concurrency_annotation_matches_runtime_type() -> None:
    annotation = inspect.signature(MessageBus.__init__).parameters["event_concurrency"].annotation

    assert annotation == EventConcurrency


def test_invalid_event_concurrency_raises() -> None:
    with pytest.raises(HandlerRegistrationError):
        MessageBus(event_concurrency=cast(Any, "bogus"))


@pytest.mark.asyncio
async def test_aclose_stops_background_loop_created_by_sync_bridge() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name

    bus.register_command_handler(AddUser, handler)
    assert bus.send_sync(AddUser(name="ada")) == "ada"

    await bus.aclose()

    assert bus._loop is None
    assert bus._thread is None
