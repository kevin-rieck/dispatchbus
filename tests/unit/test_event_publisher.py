from dataclasses import dataclass

import pytest

from dispatchbus.event_publisher import EventPublisher
from dispatchbus.exceptions import EventPublicationError, InvalidMessageError
from dispatchbus.messages import (
    EventBase,
    MessageMetadata,
    as_runtime_message,
    get_metadata,
    new_root_metadata,
)
from dispatchbus.observability import DispatchStarted
from dispatchbus.registry import HandlerRegistry
from dispatchbus.runtime import EventDispatchOutcome, MessageRuntime


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
class PlainUserAdded(EventBase):
    message_name = "user.added"
    user_id: int


@pytest.mark.asyncio
async def test_event_publisher_aggregates_follow_up_failures() -> None:
    registry = HandlerRegistry()
    runtime = MessageRuntime(event_concurrency="sequential")
    publisher = EventPublisher(registry=registry, runtime=runtime, middleware=[], subscribers=[])

    async def emitter(event: UserAdded, context) -> None:
        if event.user_id == 1:
            context.emit(UserAdded(user_id=2))

    async def failing(event: UserAdded) -> None:
        if event.user_id == 2:
            raise RuntimeError("follow-up boom")

    registry.register_event_handler(UserAdded, emitter)
    registry.register_event_handler(UserAdded, failing)

    with pytest.raises(EventPublicationError, match=r"1 event handler\(s\) failed"):
        await publisher.publish(UserAdded(user_id=1, _metadata=new_root_metadata()))


@pytest.mark.asyncio
async def test_event_publisher_stamps_follow_up_events_from_parent_metadata() -> None:
    registry = HandlerRegistry()
    runtime = MessageRuntime(event_concurrency="sequential")
    seen: list[UserAdded] = []
    observed: list[DispatchStarted] = []

    async def subscriber(event: object) -> None:
        if isinstance(event, DispatchStarted) and isinstance(event.message, UserAdded):
            observed.append(event)

    publisher = EventPublisher(
        registry=registry,
        runtime=runtime,
        middleware=[],
        subscribers=[subscriber],
    )

    async def emitter(event: UserAdded, context) -> None:
        if event.user_id == 1:
            context.emit(UserAdded(user_id=2))

    async def sink(event: UserAdded) -> None:
        seen.append(event)

    registry.register_event_handler(UserAdded, emitter)
    registry.register_event_handler(UserAdded, sink)

    root = UserAdded(user_id=1, _metadata=new_root_metadata(correlation_id="corr-99"))
    await publisher.publish(root)

    follow_up = next(event for event in seen if event.user_id == 2)
    follow_up_started = next(event for event in observed if event.message.user_id == 2)
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(follow_up)
    assert follow_up_started.metadata.correlation_id == "corr-99"
    assert follow_up_started.metadata.causation_id == root.metadata.message_id


@pytest.mark.asyncio
async def test_event_publisher_stamps_plain_follow_up_events_from_parent_metadata() -> None:
    registry = HandlerRegistry()
    runtime = MessageRuntime(event_concurrency="sequential")
    seen: list[PlainUserAdded] = []
    observed: list[DispatchStarted] = []

    async def subscriber(event: object) -> None:
        if isinstance(event, DispatchStarted) and isinstance(event.message, PlainUserAdded):
            observed.append(event)

    publisher = EventPublisher(
        registry=registry,
        runtime=runtime,
        middleware=[],
        subscribers=[subscriber],
    )

    async def emitter(event: PlainUserAdded, context) -> None:
        if event.user_id == 1:
            context.emit(PlainUserAdded(user_id=2))

    async def sink(event: PlainUserAdded) -> None:
        seen.append(event)

    registry.register_event_handler(PlainUserAdded, emitter)
    registry.register_event_handler(PlainUserAdded, sink)

    root = as_runtime_message(PlainUserAdded(user_id=1))
    await publisher.publish(root)

    follow_up = next(event for event in seen if event.user_id == 2)
    follow_up_started = next(event for event in observed if event.message.user_id == 2)
    with pytest.raises(ValueError, match="metadata"):
        get_metadata(follow_up)
    assert follow_up_started.metadata.correlation_id == get_metadata(root).correlation_id
    assert follow_up_started.metadata.causation_id == get_metadata(root).message_id


def test_event_dispatch_outcome_exposes_handler_outcomes_and_failures() -> None:
    outcome = EventDispatchOutcome(handler_outcomes=(), failures=())

    assert outcome.handler_outcomes == ()
    assert outcome.failures == ()
