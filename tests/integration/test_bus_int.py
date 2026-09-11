import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial

import pytest

from dispatchbus.bus import MessageBus
from dispatchbus.exceptions import (
    BusDrainingError,
    BusUsageError,
    EventPublicationError,
    InvalidMessageError,
)
from dispatchbus.messages import (
    CommandBase,
    EventBase,
    MessageMetadata,
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
from dispatchbus.sync_bridge import SyncBridge


@dataclass(frozen=True)
class AddUser(CommandBase):
    message_name = "user.add"

    name: str
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("AddUser is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "AddUser":
        return AddUser(name=self.name, _metadata=metadata)


@dataclass(frozen=True)
class UserAdded(EventBase):
    message_name = "user.added"

    user_id: int
    _metadata: MessageMetadata | None = None

    @property
    def metadata(self) -> MessageMetadata:
        if self._metadata is None:
            raise InvalidMessageError("UserAdded is unstamped")
        return self._metadata

    def with_metadata(self, metadata: MessageMetadata) -> "UserAdded":
        return UserAdded(user_id=self.user_id, _metadata=metadata)


@dataclass(frozen=True)
class PlainAddUser(CommandBase):
    message_name = "user.add"
    name: str


@dataclass(frozen=True)
class PlainUserAdded(EventBase):
    message_name = "user.added"
    user_id: int


def root_add_user(name: str) -> AddUser:
    return AddUser(name=name, _metadata=new_root_metadata())


def root_user_added(user_id: int) -> UserAdded:
    return UserAdded(user_id=user_id, _metadata=new_root_metadata())


@pytest.mark.asyncio
async def test_send_legacy_pre_stamped_command_preserves_payload_object_and_metadata() -> None:
    bus = MessageBus()
    seen: list[AddUser] = []

    async def handler(command: AddUser) -> str:
        seen.append(command)
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    command = AddUser(
        name="ada",
        _metadata=new_root_metadata(correlation_id="trace-123", message_id="msg-123"),
    )

    assert await bus.send(command) == "ADA"
    assert seen == [command]
    assert seen[0].metadata.message_id == "msg-123"
    assert seen[0].metadata.correlation_id == "trace-123"


@pytest.mark.asyncio
async def test_send_plain_command_propagates_correlation_and_causation_to_emitted_events() -> None:
    seen: list[PlainUserAdded] = []
    observed: list[DispatchStarted] = []

    async def subscriber(event: object) -> None:
        if isinstance(event, DispatchStarted) and isinstance(event.message, PlainUserAdded):
            observed.append(event)

    bus = MessageBus(event_concurrency="sequential", subscribers=[subscriber])

    async def command_handler(command: PlainAddUser, context) -> str:
        context.emit(PlainUserAdded(user_id=3))
        return command.name.upper()

    async def event_handler(event: PlainUserAdded) -> None:
        seen.append(event)

    bus.register_command_handler(PlainAddUser, command_handler)
    bus.register_event_handler(PlainUserAdded, event_handler)

    command = PlainAddUser(name="ada")
    await bus.send(command)

    emitted = seen[0]
    emitted_started = observed[0]
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(emitted)
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(command)
    assert emitted_started.message is emitted
    assert emitted_started.metadata.message_id
    assert emitted_started.metadata.causation_id is not None
    assert emitted_started.metadata.correlation_id == emitted_started.metadata.causation_id


@pytest.mark.asyncio
async def test_event_context_preserves_pre_stamped_legacy_event_metadata() -> None:
    bus = MessageBus(event_concurrency="sequential")
    seen: list[UserAdded] = []

    async def command_handler(command: AddUser, context) -> str:
        context.emit(
            UserAdded(user_id=3, _metadata=new_root_metadata(correlation_id="legacy-corr"))
        )
        return command.name.upper()

    async def event_handler(event: UserAdded) -> None:
        seen.append(event)

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    await bus.send(AddUser(name="ada", _metadata=new_root_metadata(correlation_id="root-corr")))

    assert seen[0].metadata.correlation_id == "legacy-corr"


@pytest.mark.asyncio
async def test_send_propagates_correlation_and_causation_to_emitted_events() -> None:
    seen: list[EventBase] = []
    observed: list[DispatchStarted] = []

    async def subscriber(event: object) -> None:
        if isinstance(event, DispatchStarted):
            observed.append(event)

    bus = MessageBus(event_concurrency="sequential", subscribers=[subscriber])

    @dataclass(frozen=True)
    class AddStampedUser(CommandBase):
        message_name = "user.add"
        name: str
        _metadata: MessageMetadata | None = None

        @property
        def metadata(self) -> MessageMetadata:
            if self._metadata is None:
                raise ValueError("unstamped")
            return self._metadata

        def with_metadata(self, metadata: MessageMetadata) -> "AddStampedUser":
            return AddStampedUser(name=self.name, _metadata=metadata)

    @dataclass(frozen=True)
    class UserStampedAdded(EventBase):
        message_name = "user.added"
        user_id: int
        _metadata: MessageMetadata | None = None

        @property
        def metadata(self) -> MessageMetadata:
            if self._metadata is None:
                raise ValueError("unstamped")
            return self._metadata

        def with_metadata(self, metadata: MessageMetadata) -> "UserStampedAdded":
            return UserStampedAdded(user_id=self.user_id, _metadata=metadata)

    async def command_handler(command: AddStampedUser, context) -> str:
        context.emit(UserStampedAdded(user_id=3))
        return command.name.upper()

    async def event_handler(event: UserStampedAdded) -> None:
        seen.append(event)

    bus.register_command_handler(AddStampedUser, command_handler)
    bus.register_event_handler(UserStampedAdded, event_handler)

    command = AddStampedUser(name="ada", _metadata=new_root_metadata(correlation_id="trace-1"))
    assert await bus.send(command) == "ADA"

    emitted = seen[0]
    emitted_started = next(
        event
        for event in observed
        if isinstance(event.message, UserStampedAdded) and event.message.user_id == 3
    )
    with pytest.raises(ValueError):
        get_metadata(emitted)
    assert emitted_started.metadata.correlation_id == "trace-1"
    assert emitted_started.metadata.causation_id == command.metadata.message_id


@pytest.mark.asyncio
async def test_emit_rejects_non_event_instances() -> None:
    bus = MessageBus()

    @dataclass(frozen=True)
    class AddStampedUser(CommandBase):
        message_name = "user.add"
        name: str
        _metadata: MessageMetadata | None = None

        @property
        def metadata(self) -> MessageMetadata:
            if self._metadata is None:
                raise ValueError("unstamped")
            return self._metadata

        def with_metadata(self, metadata: MessageMetadata) -> "AddStampedUser":
            return AddStampedUser(name=self.name, _metadata=metadata)

    async def handler(command: AddStampedUser, context) -> str:
        context.emit({"user_id": 1})
        return command.name

    bus.register_command_handler(AddStampedUser, handler)

    with pytest.raises(InvalidMessageError, match="EventBase"):
        await bus.send(AddStampedUser(name="ada", _metadata=new_root_metadata()))


@pytest.mark.asyncio
async def test_send_uses_async_command_handler() -> None:
    bus = MessageBus()

    async def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    result = await bus.send(root_add_user(name="ada"))

    assert result == "ADA"


@pytest.mark.asyncio
async def test_send_uses_sync_command_handler() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name.lower()

    bus.register_command_handler(AddUser, handler)

    result = await bus.send(root_add_user(name="ADA"))

    assert result == "ada"


@pytest.mark.asyncio
async def test_command_handler_can_emit_one_follow_up_event() -> None:
    bus = MessageBus()
    seen: list[str] = []

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=len(command.name)))
        return command.name.upper()

    async def event_handler(event: UserAdded) -> None:
        seen.append(f"event:{event.user_id}")

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    result = await bus.send(root_add_user(name="ada"))

    assert result == "ADA"
    assert seen == ["event:3"]


@pytest.mark.asyncio
async def test_command_handler_can_emit_multiple_follow_up_events_in_order() -> None:
    bus = MessageBus()
    seen: list[int] = []

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=1))
        context.emit(UserAdded(user_id=2))
        return command.name

    async def event_handler(event: UserAdded) -> None:
        seen.append(event.user_id)

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    result = await bus.send(root_add_user(name="ada"))

    assert result == "ada"
    assert seen == [1, 2]


@pytest.mark.asyncio
async def test_sequential_command_follow_up_events_use_breadth_first_order() -> None:
    bus = MessageBus(event_concurrency="sequential")
    seen: list[int] = []

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=1))
        context.emit(UserAdded(user_id=2))
        return command.name

    async def event_handler(event: UserAdded, context) -> None:
        seen.append(event.user_id)
        if event.user_id == 1:
            context.emit(UserAdded(user_id=10))

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    await bus.send(root_add_user(name="ada"))

    assert seen == [1, 2, 10]


@pytest.mark.asyncio
async def test_concurrent_command_follow_up_events_start_without_waiting_for_siblings() -> None:
    bus = MessageBus()
    seen: list[int] = []

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=1))
        context.emit(UserAdded(user_id=2))
        return command.name

    async def event_handler(event: UserAdded) -> None:
        if event.user_id == 1:
            await asyncio.sleep(0.01)
        seen.append(event.user_id)

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    await bus.send(root_add_user(name="ada"))

    assert seen == [2, 1]


@pytest.mark.asyncio
async def test_event_handler_can_emit_follow_up_events() -> None:
    bus = MessageBus(event_concurrency="sequential")
    seen: list[str] = []

    async def first_handler(event: UserAdded, context) -> None:
        seen.append(f"first:{event.user_id}")
        if event.user_id == 1:
            context.emit(UserAdded(user_id=2))

    async def second_handler(event: UserAdded) -> None:
        seen.append(f"second:{event.user_id}")

    bus.register_event_handler(UserAdded, first_handler)
    bus.register_event_handler(UserAdded, second_handler)

    await bus.publish(root_user_added(user_id=1))

    assert seen == [
        "first:1",
        "second:1",
        "first:2",
        "second:2",
    ]


@pytest.mark.asyncio
async def test_existing_one_argument_handlers_still_work() -> None:
    bus = MessageBus()

    async def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    assert await bus.send(root_add_user(name="ada")) == "ADA"


@pytest.mark.asyncio
async def test_handler_with_optional_second_positional_arg_is_not_treated_as_context_aware() -> (
    None
):
    bus = MessageBus()

    async def handler(command: AddUser, prefix: str = "X") -> str:
        return prefix + command.name

    bus.register_command_handler(AddUser, handler)

    assert await bus.send(root_add_user(name="ada")) == "Xada"


@pytest.mark.asyncio
async def test_handler_with_defaulted_context_parameter_is_not_treated_as_context_aware() -> None:
    bus = MessageBus()

    async def handler(command: AddUser, context: str = "X") -> str:
        return context + command.name

    bus.register_command_handler(AddUser, handler)

    assert await bus.send(root_add_user(name="ada")) == "Xada"


@pytest.mark.asyncio
async def test_partial_of_context_aware_handler_receives_context() -> None:
    bus = MessageBus()
    seen: list[int] = []

    async def handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=len(command.name)))
        return command.name.upper()

    async def event_handler(event: UserAdded) -> None:
        seen.append(event.user_id)

    bus.register_command_handler(AddUser, partial(handler))
    bus.register_event_handler(UserAdded, event_handler)

    assert await bus.send(root_add_user(name="ada")) == "ADA"
    assert seen == [3]


@pytest.mark.asyncio
async def test_handler_with_keyword_only_context_receives_event_context() -> None:
    bus = MessageBus(event_concurrency="sequential")
    seen: list[int] = []

    async def handler(event: UserAdded, *, context) -> None:
        seen.append(event.user_id)
        if event.user_id == 1:
            context.emit(UserAdded(user_id=2))

    bus.register_event_handler(UserAdded, handler)

    await bus.publish(root_user_added(user_id=1))

    assert seen == [1, 2]


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

    await bus.publish(root_user_added(user_id=7))

    assert sorted(seen) == ["async:7", "sync:7"]


@pytest.mark.asyncio
async def test_emitted_events_are_discarded_when_emitting_handler_fails() -> None:
    bus = MessageBus()
    seen: list[int] = []

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=99))
        raise ValueError("boom")

    async def event_handler(event: UserAdded) -> None:
        seen.append(event.user_id)

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    with pytest.raises(ValueError, match="boom"):
        await bus.send(root_add_user(name="ada"))

    assert seen == []


@pytest.mark.asyncio
async def test_emitted_event_publication_failure_propagates_from_send() -> None:
    bus = MessageBus()

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=7))
        return command.name

    async def failing_event_handler(event: UserAdded) -> None:
        raise ValueError("event boom")

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, failing_event_handler)

    with pytest.raises(EventPublicationError) as exc_info:
        await bus.send(root_add_user(name="ada"))

    assert len(exc_info.value.failures) == 1
    assert isinstance(exc_info.value.failures[0], ValueError)


@pytest.mark.asyncio
async def test_send_attempts_later_emitted_events_after_earlier_publication_failure() -> None:
    bus = MessageBus()
    seen: list[int] = []

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=1))
        context.emit(UserAdded(user_id=2))
        return command.name

    async def handler(event: UserAdded) -> None:
        seen.append(event.user_id)
        if event.user_id == 1:
            raise ValueError("first event boom")

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, handler)

    with pytest.raises(EventPublicationError) as exc_info:
        await bus.send(root_add_user(name="ada"))

    assert seen == [1, 2]
    assert [type(failure) for failure in exc_info.value.failures] == [ValueError]
    assert [str(failure) for failure in exc_info.value.failures] == ["first event boom"]


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
        await bus.publish(root_user_added(user_id=3))

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

    result = await bus.send(root_add_user(name="ada"))

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

    await bus.publish(root_user_added(user_id=9))

    assert events == ["before", "handler:9", "after"]


@pytest.mark.asyncio
async def test_publish_with_no_subscribers_is_a_no_op() -> None:
    bus = MessageBus()

    await bus.publish(root_user_added(user_id=11))


@pytest.mark.asyncio
async def test_subscribers_receive_payload_message_and_matching_metadata() -> None:
    seen: list[object] = []

    async def subscriber(event: object) -> None:
        if isinstance(event, DispatchStarted):
            seen.append(event)

    bus = MessageBus(subscribers=[subscriber])

    async def handler(command: PlainAddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(PlainAddUser, handler)

    command = PlainAddUser(name="ada")
    result = await bus.send(command)

    assert result == "ADA"
    started = seen[0]
    assert isinstance(started, DispatchStarted)
    assert started.message is command
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(command)
    assert started.metadata.message_id


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

    result = await bus.send(root_add_user(name="ada"))

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
    command = root_add_user(name="ada")

    result = await bus.send(command)

    assert result == "ADA"
    assert [type(event) for event in seen] == [
        DispatchStarted,
        HandlerStarted,
        HandlerFinished,
        DispatchFinished,
    ]
    assert all(event.message is command for event in seen)
    assert all(event.metadata == command.metadata for event in seen)
    assert all(event.message_type is AddUser for event in seen)
    assert all(event.operation == "send" for event in seen)
    dispatch_ids = {event.dispatch_id for event in seen}
    assert len(dispatch_ids) == 1
    assert seen[0].handler_count == 1
    assert seen[1].handler is handler
    assert seen[2].handler is handler
    assert seen[1].handler_name == seen[2].handler_name
    assert seen[2].duration_ms >= 0
    assert seen[3].success is True
    assert seen[3].duration_ms >= 0


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

    await bus.publish(root_user_added(user_id=7))

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
async def test_original_and_emitted_event_failures_are_both_reported() -> None:
    bus = MessageBus(event_concurrency="sequential")

    async def emitter(event: UserAdded, context) -> None:
        if event.user_id == 1:
            context.emit(UserAdded(user_id=2))

    async def fail_original(event: UserAdded) -> None:
        if event.user_id == 1:
            raise ValueError("original boom")

    async def fail_emitted(event: UserAdded) -> None:
        if event.user_id == 2:
            raise RuntimeError("emitted boom")

    bus.register_event_handler(UserAdded, emitter)
    bus.register_event_handler(UserAdded, fail_original)
    bus.register_event_handler(UserAdded, fail_emitted)

    with pytest.raises(EventPublicationError) as exc_info:
        await bus.publish(root_user_added(user_id=1))

    assert [type(failure) for failure in exc_info.value.failures] == [ValueError, RuntimeError]
    assert [str(failure) for failure in exc_info.value.failures] == [
        "original boom",
        "emitted boom",
    ]


@pytest.mark.asyncio
async def test_original_failures_preserve_when_emitted_publish_raises_error() -> None:
    async def middleware(message, call_next):
        if isinstance(message, UserAdded) and message.user_id == 2:
            raise RuntimeError("mw boom")
        return await call_next(message)

    bus = MessageBus(middleware=[middleware], event_concurrency="sequential")

    async def emitter(event: UserAdded, context) -> None:
        if event.user_id == 1:
            context.emit(UserAdded(user_id=2))

    async def fail_original(event: UserAdded) -> None:
        if event.user_id == 1:
            raise ValueError("original boom")

    bus.register_event_handler(UserAdded, emitter)
    bus.register_event_handler(UserAdded, fail_original)

    with pytest.raises(EventPublicationError) as exc_info:
        await bus.publish(root_user_added(user_id=1))

    assert [type(failure) for failure in exc_info.value.failures] == [ValueError, RuntimeError]
    assert [str(failure) for failure in exc_info.value.failures] == [
        "original boom",
        "mw boom",
    ]


@pytest.mark.asyncio
async def test_sequential_publish_drops_follow_up_events_when_middleware_fails_after_dispatch() -> (
    None
):
    async def middleware(message, call_next):
        result = await call_next(message)
        if isinstance(message, UserAdded) and message.user_id == 1:
            raise RuntimeError("mw boom")
        return result

    bus = MessageBus(middleware=[middleware], event_concurrency="sequential")
    seen: list[int] = []

    async def emitter(event: UserAdded, context) -> None:
        seen.append(event.user_id)
        if event.user_id == 1:
            context.emit(UserAdded(user_id=2))

    bus.register_event_handler(UserAdded, emitter)

    with pytest.raises(EventPublicationError) as exc_info:
        await bus.publish(root_user_added(user_id=1))

    assert [str(failure) for failure in exc_info.value.failures] == ["mw boom"]
    assert seen == [1]


@pytest.mark.asyncio
async def test_successful_event_handlers_still_publish_emitted_events_when_a_sibling_fails() -> (
    None
):
    bus = MessageBus(event_concurrency="sequential")
    seen: list[str] = []

    async def emitter(event: UserAdded, context) -> None:
        seen.append(f"emitter:{event.user_id}")
        if event.user_id == 1:
            context.emit(UserAdded(user_id=2))

    async def failing(event: UserAdded) -> None:
        seen.append(f"failing:{event.user_id}")
        if event.user_id == 1:
            raise ValueError("boom")

    async def sink(event: UserAdded) -> None:
        seen.append(f"sink:{event.user_id}")

    bus.register_event_handler(UserAdded, emitter)
    bus.register_event_handler(UserAdded, failing)
    bus.register_event_handler(UserAdded, sink)

    with pytest.raises(EventPublicationError):
        await bus.publish(root_user_added(user_id=1))

    assert seen == [
        "emitter:1",
        "failing:1",
        "sink:1",
        "emitter:2",
        "failing:2",
        "sink:2",
    ]


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
        await bus.publish(root_user_added(user_id=8))

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
async def test_concurrent_event_handlers_can_interleave_emitted_follow_up_events() -> None:
    bus = MessageBus(event_concurrency="concurrent")
    seen: list[str] = []
    first_started = asyncio.Event()
    release = asyncio.Event()

    async def first_handler(event: UserAdded, context) -> None:
        if event.user_id != 1:
            return
        seen.append("first:start")
        first_started.set()
        await release.wait()
        context.emit(UserAdded(user_id=10))
        seen.append("first:end")

    async def second_handler(event: UserAdded, context) -> None:
        if event.user_id != 1:
            return
        await first_started.wait()
        seen.append("second:start")
        context.emit(UserAdded(user_id=20))
        release.set()
        seen.append("second:end")

    async def sink(event: UserAdded) -> None:
        if event.user_id in {10, 20}:
            seen.append(f"sink:{event.user_id}")

    bus.register_event_handler(UserAdded, first_handler)
    bus.register_event_handler(UserAdded, second_handler)
    bus.register_event_handler(UserAdded, sink)

    await bus.publish(root_user_added(user_id=1))

    assert "sink:10" in seen
    assert "sink:20" in seen
    assert seen.index("second:end") < seen.index("first:end")


@pytest.mark.asyncio
async def test_concurrent_handlers_publish_follow_up_events_without_waiting() -> None:
    bus = MessageBus(event_concurrency="concurrent")
    waiting_started = asyncio.Event()
    follow_up_ran = asyncio.Event()
    seen: list[str] = []

    async def waiting_handler(event: UserAdded) -> None:
        if event.user_id != 1:
            return
        seen.append("waiting:start")
        waiting_started.set()
        await follow_up_ran.wait()
        seen.append("waiting:end")

    async def emitting_handler(event: UserAdded, context) -> None:
        if event.user_id != 1:
            return
        await waiting_started.wait()
        seen.append("emitter:start")
        context.emit(UserAdded(user_id=2))
        seen.append("emitter:end")

    async def follow_up_handler(event: UserAdded) -> None:
        if event.user_id != 2:
            return
        seen.append("follow-up")
        follow_up_ran.set()

    bus.register_event_handler(UserAdded, waiting_handler)
    bus.register_event_handler(UserAdded, emitting_handler)
    bus.register_event_handler(UserAdded, follow_up_handler)

    await asyncio.wait_for(bus.publish(root_user_added(user_id=1)), timeout=1)

    assert seen == [
        "waiting:start",
        "emitter:start",
        "emitter:end",
        "follow-up",
        "waiting:end",
    ]


@pytest.mark.asyncio
async def test_added_subscriber_sees_command_and_follow_up_event_dispatches() -> None:
    seen: list[str] = []

    async def subscriber(event: object) -> None:
        if isinstance(event, DispatchStarted):
            seen.append(event.operation)

    bus = MessageBus(event_concurrency="sequential")
    bus.add_subscriber(subscriber)

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=len(command.name)))
        return command.name

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, lambda event: None)

    assert await bus.send(root_add_user(name="ada")) == "ada"
    assert seen == ["send", "publish"]


@pytest.mark.asyncio
async def test_subscribers_see_nested_follow_up_publishes_as_normal_dispatches() -> None:
    seen: list[tuple[str, str]] = []

    async def subscriber(event: object) -> None:
        if isinstance(event, DispatchStarted | DispatchFinished):
            seen.append((type(event).__name__, event.operation))

    bus = MessageBus(subscribers=[subscriber], event_concurrency="sequential")

    async def command_handler(command: AddUser, context) -> str:
        context.emit(UserAdded(user_id=5))
        return command.name

    async def event_handler(event: UserAdded) -> None:
        return None

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    assert await bus.send(root_add_user(name="ada")) == "ada"
    assert seen == [
        ("DispatchStarted", "send"),
        ("DispatchStarted", "publish"),
        ("DispatchFinished", "publish"),
        ("DispatchFinished", "send"),
    ]


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

    await bus.publish(root_user_added(user_id=9))

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

    result = await bus.send(root_add_user(name="ada"))

    assert result == "ADA"
    assert seen == ["DispatchStarted", "HandlerStarted", "HandlerFinished", "DispatchFinished"]


@pytest.mark.asyncio
async def test_sync_handler_and_subscriber_use_the_caller_executor() -> None:
    thread_ids: list[int] = []

    def subscriber(event: object) -> None:
        thread_ids.append(threading.get_ident())

    def handler(command: AddUser) -> str:
        thread_ids.append(threading.get_ident())
        return command.name.upper()

    with ThreadPoolExecutor(max_workers=1) as executor:
        bus = MessageBus(subscribers=[subscriber], executor=executor)
        bus.register_command_handler(AddUser, handler)

        assert await bus.send(root_add_user(name="ada")) == "ADA"
        await bus.aclose()

    assert len(thread_ids) == 5
    assert len(set(thread_ids)) == 1


def test_send_sync_runs_command_through_background_runtime() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return f"sync:{command.name}"

    bus.register_command_handler(AddUser, handler)

    result = bus.send_sync(root_add_user(name="ada"))

    assert result == "sync:ada"
    bus.close()


def test_publish_sync_runs_event_handlers_through_background_runtime() -> None:
    bus = MessageBus()
    seen: list[str] = []

    def handler(event: UserAdded) -> None:
        seen.append(f"event:{event.user_id}")

    bus.register_event_handler(UserAdded, handler)

    bus.publish_sync(root_user_added(user_id=21))

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

    await bus.publish(root_user_added(user_id=1))

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

    await bus.publish(root_user_added(user_id=2))

    assert seen == ["first:start", "second:start", "second:end", "first:end"]


@pytest.mark.asyncio
async def test_publish_handles_deep_emitted_event_chains_without_recursion_error() -> None:
    bus = MessageBus(event_concurrency="sequential")
    seen: list[int] = []
    limit = 1500

    async def handler(event: UserAdded, context) -> None:
        seen.append(event.user_id)
        if event.user_id < limit:
            context.emit(UserAdded(user_id=event.user_id + 1))

    bus.register_event_handler(UserAdded, handler)

    await bus.publish(root_user_added(user_id=1))

    assert seen[0] == 1
    assert seen[-1] == limit
    assert len(seen) == limit


@pytest.mark.asyncio
async def test_concurrent_publish_aggregates_sibling_and_follow_up_failures() -> None:
    bus = MessageBus(event_concurrency="concurrent")

    async def emitter(event: UserAdded, context) -> None:
        if event.user_id == 1:
            context.emit(UserAdded(user_id=2))

    async def sibling_failure(event: UserAdded) -> None:
        if event.user_id == 1:
            raise ValueError("sibling boom")

    async def follow_up_failure(event: UserAdded) -> None:
        if event.user_id == 2:
            raise RuntimeError("follow-up boom")

    bus.register_event_handler(UserAdded, emitter)
    bus.register_event_handler(UserAdded, sibling_failure)
    bus.register_event_handler(UserAdded, follow_up_failure)

    with pytest.raises(EventPublicationError) as exc_info:
        await bus.publish(root_user_added(user_id=1))

    assert [type(failure) for failure in exc_info.value.failures] == [ValueError, RuntimeError]
    assert [str(failure) for failure in exc_info.value.failures] == [
        "sibling boom",
        "follow-up boom",
    ]


@pytest.mark.asyncio
async def test_aclose_stops_background_loop_created_by_sync_bridge() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name

    bus.register_command_handler(AddUser, handler)
    assert await asyncio.to_thread(bus.send_sync, root_add_user(name="ada")) == "ada"

    await bus.aclose()

    assert bus._sync_bridge._loop is None
    assert bus._sync_bridge._thread is None


@pytest.mark.asyncio
async def test_aclose_waits_for_inflight_sync_bridge_work() -> None:
    bus = MessageBus()
    started = threading.Event()
    release = threading.Event()
    result: dict[str, str] = {}

    def handler(command: AddUser) -> str:
        started.set()
        release.wait()
        return command.name.upper()

    def run_send() -> None:
        result["value"] = bus.send_sync(root_add_user(name="ada"))

    bus.register_command_handler(AddUser, handler)

    worker = threading.Thread(target=run_send)
    worker.start()
    assert started.wait(timeout=1)

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    assert close_task.done() is False

    release.set()

    await asyncio.wait_for(close_task, timeout=1)
    worker.join(timeout=1)

    assert result["value"] == "ADA"
    assert bus._sync_bridge._loop is None
    assert bus._sync_bridge._thread is None


@pytest.mark.asyncio
async def test_send_rejects_new_work_once_aclose_starts() -> None:
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(command: AddUser) -> str:
        started.set()
        await release.wait()
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    first_send = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await bus.send(root_add_user(name="grace"))

    release.set()

    assert await first_send == "ADA"
    await close_task


@pytest.mark.asyncio
async def test_publish_rejects_new_work_once_aclose_starts() -> None:
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(event: UserAdded) -> None:
        started.set()
        await release.wait()

    bus.register_event_handler(UserAdded, handler)

    first_publish = asyncio.create_task(bus.publish(root_user_added(user_id=1)))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await bus.publish(root_user_added(user_id=2))

    release.set()

    await first_publish
    await close_task


@pytest.mark.asyncio
async def test_accepted_work_can_call_public_publish_during_drain() -> None:
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()
    seen: list[int] = []

    async def command_handler(command: AddUser) -> str:
        started.set()
        await release.wait()
        await bus.publish(root_user_added(user_id=len(command.name)))
        return command.name.upper()

    async def event_handler(event: UserAdded) -> None:
        seen.append(event.user_id)

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    first_send = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    release.set()

    assert await first_send == "ADA"
    assert seen == [3]
    await close_task


@pytest.mark.asyncio
async def test_child_task_publish_from_accepted_work_is_allowed_during_drain() -> None:
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()
    detached_started = asyncio.Event()
    event_started = asyncio.Event()
    event_release = asyncio.Event()
    detached_publish: asyncio.Task[None] | None = None
    seen: list[int] = []

    async def command_handler(command: AddUser) -> str:
        nonlocal detached_publish
        started.set()
        await release.wait()

        async def detached() -> None:
            detached_started.set()
            await bus.publish(root_user_added(user_id=len(command.name)))

        detached_publish = asyncio.create_task(detached())
        await detached_started.wait()
        await asyncio.sleep(0)
        return command.name.upper()

    async def event_handler(event: UserAdded) -> None:
        event_started.set()
        await event_release.wait()
        seen.append(event.user_id)

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    send_task = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    assert close_task.done() is False

    release.set()

    await detached_started.wait()
    await asyncio.sleep(0)
    assert detached_publish is not None
    await event_started.wait()
    await asyncio.sleep(0)
    assert close_task.done() is False

    event_release.set()

    assert await send_task == "ADA"
    await detached_publish
    await close_task
    assert seen == [3]


@pytest.mark.asyncio
async def test_nested_send_from_accepted_work_is_rejected_during_drain() -> None:
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()

    @dataclass(frozen=True)
    class AddAdmin(CommandBase):
        message_name = "admin.add"

        name: str
        _metadata: MessageMetadata | None = None

        @property
        def metadata(self) -> MessageMetadata:
            if self._metadata is None:
                raise InvalidMessageError("AddAdmin is unstamped")
            return self._metadata

        def with_metadata(self, metadata: MessageMetadata) -> "AddAdmin":
            return AddAdmin(name=self.name, _metadata=metadata)

    async def handler(command: AddUser) -> str:
        started.set()
        await release.wait()
        with pytest.raises(BusDrainingError, match="message bus is draining"):
            await bus.send(AddAdmin(name="grace", _metadata=new_root_metadata()))
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    send_task = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)

    release.set()

    assert await send_task == "ADA"
    await close_task


@pytest.mark.asyncio
async def test_child_task_send_from_accepted_work_is_rejected_during_drain() -> None:
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()
    child_send: asyncio.Task[str] | None = None

    async def handler(command: AddUser) -> str:
        nonlocal child_send
        started.set()
        await release.wait()
        child_send = asyncio.create_task(bus.send(root_add_user(name="grace")))
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    send_task = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)

    release.set()

    assert await send_task == "ADA"
    assert child_send is not None
    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await child_send
    await close_task


@pytest.mark.asyncio
async def test_detached_tasks_are_rejected_after_bus_closes() -> None:
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()
    allow_detached_publish = asyncio.Event()
    detached_publish: asyncio.Task[None] | None = None
    seen: list[int] = []

    async def command_handler(command: AddUser) -> str:
        nonlocal detached_publish
        started.set()
        await release.wait()

        async def detached() -> None:
            await allow_detached_publish.wait()
            await bus.publish(root_user_added(user_id=len(command.name)))

        detached_publish = asyncio.create_task(detached())
        return command.name.upper()

    async def event_handler(event: UserAdded) -> None:
        seen.append(event.user_id)

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    first_send = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    release.set()

    assert await first_send == "ADA"
    await close_task

    assert detached_publish is not None
    allow_detached_publish.set()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await detached_publish

    assert seen == []


@pytest.mark.asyncio
async def test_close_from_async_context_without_background_loop_raises_usage_error() -> None:
    bus = MessageBus()

    async def handler(command: AddUser) -> str:
        return command.name

    bus.register_command_handler(AddUser, handler)
    assert await bus.send(root_add_user(name="ada")) == "ada"

    with pytest.raises(BusUsageError, match=r"use await bus\.aclose\(\) from async code"):
        bus.close()

    await bus.aclose()


@pytest.mark.asyncio
async def test_close_from_async_context_with_background_loop_raises_usage_error() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name

    bus.register_command_handler(AddUser, handler)
    assert await asyncio.to_thread(bus.send_sync, root_add_user(name="ada")) == "ada"

    with pytest.raises(BusUsageError, match=r"use await bus\.aclose\(\) from async code"):
        bus.close()

    await bus.aclose()


@pytest.mark.asyncio
async def test_send_sync_from_async_context_raises_usage_error() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    with pytest.raises(
        BusUsageError,
        match=r"send_sync\(\) cannot run inside an active event loop",
    ):
        bus.send_sync(root_add_user(name="ada"))


@pytest.mark.asyncio
async def test_publish_sync_from_async_context_raises_usage_error() -> None:
    bus = MessageBus()

    def handler(event: UserAdded) -> None:
        return None

    bus.register_event_handler(UserAdded, handler)

    with pytest.raises(
        BusUsageError,
        match=r"publish_sync\(\) cannot run inside an active event loop",
    ):
        bus.publish_sync(root_user_added(user_id=1))


@pytest.mark.asyncio
async def test_send_rejects_sync_command_handler_that_returns_awaitable() -> None:
    bus = MessageBus()

    async def inner(command: AddUser) -> str:
        return command.name.upper()

    def handler(command: AddUser):
        return inner(command)

    bus.register_command_handler(AddUser, handler)

    with pytest.raises(BusUsageError, match="sync handler returned an awaitable"):
        await bus.send(root_add_user(name="ada"))


@pytest.mark.asyncio
async def test_publish_rejects_sync_event_handler_that_returns_awaitable() -> None:
    bus = MessageBus()

    async def inner(event: UserAdded) -> None:
        return None

    def handler(event: UserAdded):
        return inner(event)

    bus.register_event_handler(UserAdded, handler)

    with pytest.raises(EventPublicationError) as exc_info:
        await bus.publish(root_user_added(user_id=1))

    assert len(exc_info.value.failures) == 1
    assert isinstance(exc_info.value.failures[0], BusUsageError)
    assert (
        str(exc_info.value.failures[0])
        == "sync handler returned an awaitable; declare it with async def"
    )


@pytest.mark.asyncio
async def test_subscriber_returning_awaitable_does_not_fail_dispatch() -> None:
    seen: list[str] = []

    async def inner(event: object) -> None:
        seen.append(type(event).__name__)

    def subscriber(event: object):
        return inner(event)

    bus = MessageBus(subscribers=[subscriber])

    async def handler(command: AddUser) -> str:
        return command.name

    bus.register_command_handler(AddUser, handler)

    assert await bus.send(root_add_user(name="ada")) == "ada"
    assert seen == []


def test_concurrent_send_sync_starts_only_one_background_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = MessageBus()
    starts = 0
    starts_lock = threading.Lock()
    entered = threading.Event()
    release = threading.Event()
    ready = threading.Barrier(3)
    original = SyncBridge._run_background_loop

    def wrapped(self: SyncBridge) -> None:
        nonlocal starts
        with starts_lock:
            starts += 1
        entered.set()
        release.wait(timeout=1)
        return original(self)

    monkeypatch.setattr(SyncBridge, "_run_background_loop", wrapped)

    def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    results: list[str] = []

    def worker(name: str) -> None:
        ready.wait()
        results.append(bus.send_sync(root_add_user(name=name)))

    first = threading.Thread(target=worker, args=("ada",))
    second = threading.Thread(target=worker, args=("grace",))
    first.start()
    second.start()
    ready.wait()
    assert entered.wait(timeout=1)
    release.set()
    first.join(timeout=1)
    second.join(timeout=1)

    assert sorted(results) == ["ADA", "GRACE"]
    assert starts == 1

    bus.close()


def test_send_sync_rejects_before_submitting_after_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = MessageBus()
    submitted = False
    original_run = SyncBridge.run

    def run(bridge: SyncBridge, coroutine, timeout=None):
        nonlocal submitted
        submitted = True
        return original_run(bridge, coroutine, timeout=timeout)

    monkeypatch.setattr(SyncBridge, "run", run)
    bus.close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        bus.send_sync(root_add_user(name="ada"))
    with pytest.raises(BusDrainingError, match="message bus is draining"):
        bus.publish_sync(root_user_added(user_id=1))

    assert submitted is False


def test_close_rejects_new_sync_work() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    assert bus.send_sync(root_add_user(name="ada")) == "ADA"

    bus.close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        bus.send_sync(root_add_user(name="grace"))


def test_send_sync_rejects_new_work_after_close() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    assert bus.send_sync(root_add_user(name="ada")) == "ADA"

    bus.close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        bus.send_sync(root_add_user(name="grace"))


def test_publish_sync_rejects_new_work_after_close() -> None:
    bus = MessageBus()
    seen: list[int] = []

    def handler(event: UserAdded) -> None:
        seen.append(event.user_id)

    bus.register_event_handler(UserAdded, handler)

    bus.publish_sync(root_user_added(user_id=1))
    assert seen == [1]

    bus.close()

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        bus.publish_sync(root_user_added(user_id=2))


@pytest.mark.asyncio
async def test_simultaneous_aclose_runs_runtime_cleanup_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    shutdown_calls = 0
    original_shutdown = executor.shutdown

    def shutdown(*, wait: bool = True, cancel_futures: bool = False) -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        original_shutdown(wait=wait, cancel_futures=cancel_futures)

    monkeypatch.setattr(executor, "shutdown", shutdown)
    monkeypatch.setattr("dispatchbus.dispatch_tree.ThreadPoolExecutor", lambda: executor)
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(command: AddUser) -> str:
        started.set()
        await release.wait()
        return command.name

    bus.register_command_handler(AddUser, handler)
    send_task = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    first_close = asyncio.create_task(bus.aclose())
    second_close = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    assert not first_close.done()
    assert not second_close.done()

    release.set()
    assert await send_task == "ada"
    await asyncio.gather(first_close, second_close)

    assert shutdown_calls == 1


def test_aclose_transition_survives_waiter_loop_shutdown() -> None:
    bus = MessageBus()
    started = threading.Event()

    async def handler(command: AddUser) -> str:
        started.set()
        await asyncio.Event().wait()
        return command.name

    bus.register_command_handler(AddUser, handler)

    async def cancel_waiter() -> None:
        send_task = asyncio.create_task(bus.send(root_add_user(name="ada")))
        await asyncio.to_thread(started.wait, 1)
        close_task = asyncio.create_task(bus.aclose())
        await asyncio.sleep(0)
        close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task
        send_task.cancel()

    asyncio.run(cancel_waiter())
    asyncio.run(bus.aclose())


@pytest.mark.asyncio
async def test_cancelled_aclose_waiter_does_not_cancel_shared_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    original_shutdown = executor.shutdown
    cleanup_finished = asyncio.Event()

    def shutdown(*, wait: bool = True, cancel_futures: bool = False) -> None:
        original_shutdown(wait=wait, cancel_futures=cancel_futures)
        cleanup_finished.set()

    monkeypatch.setattr(executor, "shutdown", shutdown)
    monkeypatch.setattr("dispatchbus.dispatch_tree.ThreadPoolExecutor", lambda: executor)
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(command: AddUser) -> str:
        started.set()
        await release.wait()
        return command.name

    bus.register_command_handler(AddUser, handler)
    send_task = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    release.set()
    assert await send_task == "ada"
    await asyncio.wait_for(cleanup_finished.wait(), timeout=1)
    await asyncio.wait_for(bus.aclose(), timeout=1)

    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await bus.send(root_add_user(name="grace"))


@pytest.mark.asyncio
async def test_cleanup_failure_closes_bus_and_is_shared_by_close_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    cleanup_error = RuntimeError("cleanup failed")

    def shutdown(*, wait: bool = True, cancel_futures: bool = False) -> None:
        raise cleanup_error

    monkeypatch.setattr(executor, "shutdown", shutdown)
    monkeypatch.setattr("dispatchbus.dispatch_tree.ThreadPoolExecutor", lambda: executor)
    bus = MessageBus()

    first_close = asyncio.create_task(bus.aclose())
    second_close = asyncio.create_task(bus.aclose())
    outcomes = await asyncio.gather(first_close, second_close, return_exceptions=True)

    assert outcomes == [cleanup_error, cleanup_error]
    assert outcomes[0] is outcomes[1]
    with pytest.raises(BusDrainingError, match="message bus is draining"):
        await bus.send(root_add_user(name="grace"))


@pytest.mark.asyncio
async def test_close_and_aclose_wait_for_the_same_sync_bridge_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name

    bus.register_command_handler(AddUser, handler)
    assert await asyncio.to_thread(bus.send_sync, root_add_user(name="ada")) == "ada"

    close_started = threading.Event()
    allow_close = threading.Event()
    original_close = SyncBridge.close

    def blocked_close(bridge: SyncBridge) -> None:
        close_started.set()
        assert allow_close.wait(timeout=1)
        original_close(bridge)

    monkeypatch.setattr(SyncBridge, "close", blocked_close)
    async_close = asyncio.create_task(bus.aclose())
    assert await asyncio.to_thread(close_started.wait, 1)

    close_errors: list[BaseException] = []

    def run_close() -> None:
        try:
            bus.close()
        except BaseException as exc:
            close_errors.append(exc)

    close_thread = threading.Thread(target=run_close, daemon=True)
    close_thread.start()
    await asyncio.sleep(0.05)
    allow_close.set()

    await asyncio.wait_for(async_close, timeout=1)
    await asyncio.to_thread(close_thread.join, 1)
    assert not close_thread.is_alive()
    assert close_errors == []


@pytest.mark.asyncio
async def test_simultaneous_aclose_closes_the_sync_bridge_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_calls = 0
    original_close = SyncBridge.close

    def close(bridge: SyncBridge) -> None:
        nonlocal close_calls
        close_calls += 1
        original_close(bridge)

    monkeypatch.setattr(SyncBridge, "close", close)
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name

    bus.register_command_handler(AddUser, handler)
    assert await asyncio.to_thread(bus.send_sync, root_add_user(name="ada")) == "ada"

    await asyncio.gather(bus.aclose(), bus.aclose())

    assert close_calls == 1


@pytest.mark.asyncio
async def test_repeated_aclose_is_harmless() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name

    bus.register_command_handler(AddUser, handler)
    assert await asyncio.to_thread(bus.send_sync, root_add_user(name="ada")) == "ada"

    await bus.aclose()
    await bus.aclose()

    assert bus._sync_bridge._loop is None
    assert bus._sync_bridge._thread is None


def test_repeated_close_is_harmless() -> None:
    bus = MessageBus()

    def handler(command: AddUser) -> str:
        return command.name

    bus.register_command_handler(AddUser, handler)
    assert bus.send_sync(root_add_user(name="ada")) == "ada"

    bus.close()
    bus.close()


@pytest.mark.asyncio
async def test_aclose_waits_for_accepted_send_to_finish() -> None:
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()
    finished: list[str] = []

    async def handler(command: AddUser) -> str:
        started.set()
        await release.wait()
        finished.append(command.name)
        return command.name.upper()

    bus.register_command_handler(AddUser, handler)

    send_task = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    assert close_task.done() is False

    release.set()

    assert await send_task == "ADA"
    await close_task
    assert finished == ["ada"]


@pytest.mark.asyncio
async def test_aclose_completes_after_command_handler_failure() -> None:
    bus = MessageBus()

    async def handler(command: AddUser) -> str:
        raise ValueError("command failed")

    bus.register_command_handler(AddUser, handler)

    with pytest.raises(ValueError, match="command failed"):
        await bus.send(root_add_user(name="ada"))

    await asyncio.wait_for(bus.aclose(), timeout=1)


@pytest.mark.asyncio
async def test_aclose_completes_after_command_cancellation() -> None:
    bus = MessageBus()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(command: AddUser) -> str:
        started.set()
        await release.wait()
        return command.name

    bus.register_command_handler(AddUser, handler)

    send_task = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()
    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    assert close_task.done() is False

    send_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await send_task

    await asyncio.wait_for(close_task, timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("event_concurrency", ["sequential", "concurrent"])
async def test_aclose_completes_after_event_publication_cancellation(
    event_concurrency: str,
) -> None:
    bus = MessageBus(event_concurrency=event_concurrency)
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(event: UserAdded) -> None:
        started.set()
        await release.wait()

    bus.register_event_handler(UserAdded, handler)

    publish_task = asyncio.create_task(bus.publish(root_user_added(user_id=1)))
    await started.wait()
    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    assert close_task.done() is False

    publish_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await publish_task

    release.set()
    await asyncio.wait_for(close_task, timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("event_concurrency", ["sequential", "concurrent"])
async def test_aclose_allows_follow_up_events_from_accepted_work(event_concurrency: str) -> None:
    bus = MessageBus(event_concurrency=event_concurrency)
    started = asyncio.Event()
    release = asyncio.Event()
    event_started = asyncio.Event()
    event_release = asyncio.Event()
    seen: list[str] = []

    async def command_handler(command: AddUser, context) -> str:
        started.set()
        await release.wait()
        context.emit(UserAdded(user_id=len(command.name)))
        seen.append("command:done")
        return command.name

    async def event_handler(event: UserAdded) -> None:
        event_started.set()
        await event_release.wait()
        seen.append(f"event:{event.user_id}")

    bus.register_command_handler(AddUser, command_handler)
    bus.register_event_handler(UserAdded, event_handler)

    send_task = asyncio.create_task(bus.send(root_add_user(name="ada")))
    await started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)

    release.set()

    await event_started.wait()
    await asyncio.sleep(0)
    assert close_task.done() is False

    event_release.set()

    assert await send_task == "ada"
    await close_task
    assert seen == ["command:done", "event:3"]


@pytest.mark.asyncio
@pytest.mark.parametrize("event_concurrency", ["sequential", "concurrent"])
async def test_aclose_allows_event_originated_follow_up_events_from_accepted_work(
    event_concurrency: str,
) -> None:
    bus = MessageBus(event_concurrency=event_concurrency)
    root_started = asyncio.Event()
    release_root = asyncio.Event()
    follow_up_started = asyncio.Event()
    release_follow_up = asyncio.Event()
    seen: list[str] = []

    async def root_handler(event: UserAdded, context) -> None:
        if event.user_id != 1:
            return
        root_started.set()
        await release_root.wait()
        context.emit(UserAdded(user_id=2))
        seen.append("root:done")

    async def follow_up_handler(event: UserAdded) -> None:
        if event.user_id != 2:
            return
        follow_up_started.set()
        await release_follow_up.wait()
        seen.append("follow-up:done")

    bus.register_event_handler(UserAdded, root_handler)
    bus.register_event_handler(UserAdded, follow_up_handler)

    publish_task = asyncio.create_task(bus.publish(root_user_added(user_id=1)))
    await root_started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    assert close_task.done() is False

    release_root.set()

    await follow_up_started.wait()
    await asyncio.sleep(0)
    assert close_task.done() is False

    release_follow_up.set()

    await publish_task
    await close_task
    assert seen == ["root:done", "follow-up:done"]


@pytest.mark.asyncio
@pytest.mark.parametrize("event_concurrency", ["sequential", "concurrent"])
async def test_event_handler_can_explicitly_publish_during_drain(event_concurrency: str) -> None:
    bus = MessageBus(event_concurrency=event_concurrency)
    root_started = asyncio.Event()
    release_root = asyncio.Event()
    seen: list[int] = []

    async def root_handler(event: UserAdded) -> None:
        if event.user_id != 1:
            return
        root_started.set()
        await release_root.wait()
        await bus.publish(root_user_added(user_id=2))

    async def follow_up_handler(event: UserAdded) -> None:
        if event.user_id == 2:
            seen.append(event.user_id)

    bus.register_event_handler(UserAdded, root_handler)
    bus.register_event_handler(UserAdded, follow_up_handler)

    publish_task = asyncio.create_task(bus.publish(root_user_added(user_id=1)))
    await root_started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    assert close_task.done() is False

    release_root.set()

    await publish_task
    await close_task
    assert seen == [2]


@pytest.mark.asyncio
@pytest.mark.parametrize("event_concurrency", ["sequential", "concurrent"])
async def test_event_child_task_can_publish_after_parent_handler_finishes_during_drain(
    event_concurrency: str,
) -> None:
    bus = MessageBus(event_concurrency=event_concurrency)
    root_started = asyncio.Event()
    child_allowed = asyncio.Event()
    follow_up_started = asyncio.Event()
    release_follow_up = asyncio.Event()
    child_event_started = asyncio.Event()
    child_task: asyncio.Task[None] | None = None

    async def root_handler(event: UserAdded, context) -> None:
        nonlocal child_task
        if event.user_id != 1:
            return

        async def detached() -> None:
            await child_allowed.wait()
            await bus.publish(root_user_added(user_id=3))

        child_task = asyncio.create_task(detached())
        root_started.set()
        context.emit(UserAdded(user_id=2))

    async def follow_up_handler(event: UserAdded) -> None:
        if event.user_id != 2:
            return
        follow_up_started.set()
        await release_follow_up.wait()

    async def child_handler(event: UserAdded) -> None:
        if event.user_id == 3:
            child_event_started.set()

    bus.register_event_handler(UserAdded, root_handler)
    bus.register_event_handler(UserAdded, follow_up_handler)
    bus.register_event_handler(UserAdded, child_handler)

    publish_task = asyncio.create_task(bus.publish(root_user_added(user_id=1)))
    await root_started.wait()
    await follow_up_started.wait()

    close_task = asyncio.create_task(bus.aclose())
    await asyncio.sleep(0)
    assert close_task.done() is False

    child_allowed.set()
    await child_event_started.wait()
    assert child_task is not None
    await child_task

    release_follow_up.set()
    await publish_task
    await close_task


@pytest.mark.asyncio
async def test_sequential_follow_up_execution_no_interleaving() -> None:
    bus = MessageBus(event_concurrency="sequential")
    seen: list[str] = []

    async def handler_1(event: UserAdded, context) -> None:
        if event.user_id != 1:
            return
        seen.append("start:1")
        await asyncio.sleep(0.01)
        context.emit(UserAdded(user_id=2))
        seen.append("end:1")

    async def handler_2(event: UserAdded) -> None:
        if event.user_id != 1:
            return
        seen.append("start:2")
        await asyncio.sleep(0.01)
        seen.append("end:2")

    async def handler_3(event: UserAdded) -> None:
        if event.user_id != 2:
            return
        seen.append("handler_3")

    bus.register_event_handler(UserAdded, handler_1)
    bus.register_event_handler(UserAdded, handler_2)
    bus.register_event_handler(UserAdded, handler_3)

    await bus.publish(root_user_added(user_id=1))

    assert seen == ["start:1", "end:1", "start:2", "end:2", "handler_3"]
