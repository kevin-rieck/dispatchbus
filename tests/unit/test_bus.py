import inspect
from dataclasses import dataclass
from typing import Any, cast

import pytest

from dispatchbus.bus import MessageBus
from dispatchbus.dispatch_tree import EventConcurrency
from dispatchbus.exceptions import (
    BusDrainingError,
    BusUsageError,
    HandlerRegistrationError,
    InvalidMessageError,
)
from dispatchbus.messages import (
    CommandBase,
    EventBase,
    MessageMetadata,
    get_metadata,
    new_root_metadata,
)


@dataclass(frozen=True)
class AddUserCommand(CommandBase):
    message_name = "user.add"

    name: str
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("AddUserCommand is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "AddUserCommand":
        return AddUserCommand(name=self.name, _metadata=metadata)


@dataclass(frozen=True)
class UserAddedEvent(EventBase):
    message_name = "user.added"

    user_id: int
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("UserAddedEvent is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "UserAddedEvent":
        return UserAddedEvent(user_id=self.user_id, _metadata=metadata)


@dataclass(frozen=True)
class PlainAddUser(CommandBase):
    message_name = "user.add"
    name: str


@dataclass(frozen=True)
class PlainUserAdded(EventBase):
    message_name = "user.added"
    user_id: int


@pytest.mark.asyncio
async def test_message_bus_copies_mutable_configuration_collections() -> None:
    middleware_seen: list[str] = []
    added_subscriber_seen: list[object] = []

    async def initial_middleware(message, call_next):
        middleware_seen.append("initial")
        return await call_next(message)

    async def added_middleware(message, call_next):
        middleware_seen.append("added")
        return await call_next(message)

    async def initial_subscriber(event: object) -> None:
        pass

    async def added_subscriber(event: object) -> None:
        added_subscriber_seen.append(event)

    middleware_stack = [initial_middleware]
    subscribers = [initial_subscriber]
    bus = MessageBus(middleware=middleware_stack, subscribers=subscribers)
    middleware_stack.append(added_middleware)
    subscribers.append(added_subscriber)

    async def handler(command: AddUserCommand) -> str:
        return command.name

    bus.register_command_handler(AddUserCommand, handler)

    assert await bus.send(AddUserCommand(name="ada")) == "ada"
    assert middleware_seen == ["initial"]
    assert added_subscriber_seen == []


def test_message_bus_event_concurrency_annotation_matches_runtime_type() -> None:
    annotation = inspect.signature(MessageBus.__init__).parameters["event_concurrency"].annotation

    assert annotation == EventConcurrency


def test_invalid_event_concurrency_raises() -> None:
    with pytest.raises(HandlerRegistrationError):
        MessageBus(event_concurrency=cast(Any, "bogus"))


@pytest.mark.asyncio
async def test_send_rejects_event_instances() -> None:
    bus = MessageBus()

    with pytest.raises(BusUsageError, match="CommandBase"):
        await bus.send(UserAddedEvent(user_id=1, _metadata=new_root_metadata()))


@pytest.mark.asyncio
async def test_publish_rejects_command_instances() -> None:
    bus = MessageBus()

    with pytest.raises(BusUsageError, match="EventBase"):
        await bus.publish(AddUserCommand(name="ada", _metadata=new_root_metadata()))


@pytest.mark.asyncio
async def test_send_accepts_legacy_unstamped_root_command() -> None:
    bus = MessageBus()
    bus.register_command_handler(AddUserCommand, lambda command: command.name)

    assert await bus.send(AddUserCommand(name="ada")) == "ada"


@pytest.mark.asyncio
async def test_dispatched_plain_payload_does_not_become_metadata_readable() -> None:
    bus = MessageBus()
    seen: list[PlainAddUser] = []

    async def handler(command: PlainAddUser) -> str:
        seen.append(command)
        return command.name.upper()

    bus.register_command_handler(PlainAddUser, handler)

    payload = PlainAddUser(name="ada")
    result = await bus.send(payload)

    assert result == "ADA"
    assert seen == [payload]
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(payload)


@pytest.mark.asyncio
async def test_publish_plain_root_event_delivers_payload() -> None:
    bus = MessageBus()
    seen: list[PlainUserAdded] = []

    async def handler(event: PlainUserAdded) -> None:
        seen.append(event)

    bus.register_event_handler(PlainUserAdded, handler)

    payload = PlainUserAdded(user_id=3)
    await bus.publish(payload)

    assert seen == [payload]
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(payload)


@pytest.mark.asyncio
async def test_async_context_manager() -> None:
    async def handler(command: AddUserCommand) -> str:
        return command.name

    async with MessageBus() as bus:
        bus.register_command_handler(AddUserCommand, handler)
        assert await bus.send(AddUserCommand(name="ada")) == "ada"

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await bus.send(AddUserCommand(name="grace"))


def test_sync_context_manager() -> None:
    def handler(command: AddUserCommand) -> str:
        return command.name

    with MessageBus() as bus:
        bus.register_command_handler(AddUserCommand, handler)
        assert bus.send_sync(AddUserCommand(name="ada")) == "ada"

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        bus.send_sync(AddUserCommand(name="grace"))


def test_caller_executor_remains_usable_after_bus_closes() -> None:
    from concurrent.futures import ThreadPoolExecutor

    executor = ThreadPoolExecutor(max_workers=2)
    bus = MessageBus(executor=executor)

    bus.close()

    assert executor.submit(lambda: "caller-owned").result() == "caller-owned"
    executor.shutdown(wait=True)


def test_bus_shuts_down_internally_created_executor(monkeypatch: pytest.MonkeyPatch) -> None:
    from concurrent.futures import ThreadPoolExecutor

    executor = ThreadPoolExecutor(max_workers=1)
    monkeypatch.setattr("dispatchbus.dispatch_tree.ThreadPoolExecutor", lambda: executor)
    bus = MessageBus()

    bus.close()

    with pytest.raises(RuntimeError, match="cannot schedule new futures after shutdown"):
        executor.submit(lambda: None)


@pytest.mark.asyncio
async def test_aclose_waits_for_concurrent_event_tasks_to_finish() -> None:
    import asyncio

    bus = MessageBus(event_concurrency="concurrent")
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(event: PlainUserAdded) -> None:
        started.set()
        await release.wait()

    bus.register_event_handler(PlainUserAdded, handler)

    publish_task = asyncio.create_task(bus.publish(PlainUserAdded(user_id=1)))
    await started.wait()
    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)

    assert not close_task.done()

    release.set()
    await publish_task
    await close_task
