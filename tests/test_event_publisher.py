from dataclasses import dataclass

import pytest

from dispatchr.event_publisher import EventPublisher
from dispatchr.exceptions import EventPublicationError
from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import EventDispatchOutcome, MessageRuntime


@dataclass(frozen=True)
class UserAdded:
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
        await publisher.publish(UserAdded(user_id=1))


def test_event_dispatch_outcome_exposes_handler_outcomes_and_failures() -> None:
    outcome = EventDispatchOutcome(handler_outcomes=(), failures=())

    assert outcome.handler_outcomes == ()
    assert outcome.failures == ()
