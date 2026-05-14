import asyncio
from dataclasses import dataclass

import pytest

from dispatchr.bus import MessageBus
from dispatchr.exceptions import EventPublicationError


@dataclass(frozen=True)
class AddUser:
    name: str


@dataclass(frozen=True)
class UserAdded:
    user_id: int


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
