from dataclasses import dataclass

import pytest

from dispatchbus import CommandBase, EventBase, MessageBus
from dispatchbus.context import EventContext
from dispatchbus.exceptions import EventPublicationError
from dispatchbus.observability import HandlerFailed


def test_message_bus_accepts_error_handler() -> None:
    def dummy_error_handler(exc, message, handler, context) -> None:
        pass

    bus = MessageBus(error_handler=dummy_error_handler)
    assert bus._runtime._error_handler is dummy_error_handler


@dataclass(frozen=True)
class BoomEvent(EventBase):
    message_name = "test.boom_event"
    val: int


@dataclass(frozen=True)
class BoomCommand(CommandBase):
    message_name = "test.boom_command"


@pytest.mark.asyncio
async def test_async_error_handler_invoked_on_event_failure() -> None:
    called_args = []

    async def async_error_handler(exc, message, handler, context) -> None:
        called_args.append((exc, message, handler, context))
        raise exc  # Re-raise to propagate

    async def failing_handler(event: BoomEvent) -> None:
        raise ValueError("boom event handler")

    bus = MessageBus(error_handler=async_error_handler)
    bus.register_event_handler(BoomEvent, failing_handler)

    with pytest.raises(EventPublicationError):
        await bus.publish(BoomEvent(val=42))

    assert len(called_args) == 1
    exc, msg, handler, ctx = called_args[0]
    assert isinstance(exc, ValueError)
    assert str(exc) == "boom event handler"
    assert isinstance(msg, BoomEvent)
    assert msg.val == 42
    assert handler == failing_handler
    assert isinstance(ctx, EventContext)


@pytest.mark.asyncio
async def test_sync_error_handler_invoked_on_command_failure() -> None:
    called_args = []

    def sync_error_handler(exc, message, handler, context) -> None:
        called_args.append((exc, message, handler, context))
        raise exc  # Re-raise to propagate

    async def failing_command_handler(command: BoomCommand) -> None:
        raise RuntimeError("boom command handler")

    bus = MessageBus(error_handler=sync_error_handler)
    bus.register_command_handler(BoomCommand, failing_command_handler)

    with pytest.raises(RuntimeError):
        await bus.send(BoomCommand())

    assert len(called_args) == 1
    exc, msg, handler, ctx = called_args[0]
    assert isinstance(exc, RuntimeError)
    assert str(exc) == "boom command handler"
    assert isinstance(msg, BoomCommand)
    assert handler == failing_command_handler
    assert isinstance(ctx, EventContext)


@pytest.mark.asyncio
async def test_error_swallowing_and_recovery() -> None:
    observability_seen = []

    async def obs_subscriber(event) -> None:
        if isinstance(event, HandlerFailed):
            observability_seen.append(event)

    async def swallowing_error_handler(exc, message, handler, context) -> None:
        # Swallow the exception by returning normally
        # Can also emit a recovery event
        if isinstance(message, BoomEvent) and message.val == 99:
            context.emit(BoomEvent(val=100))

    runs = []

    async def failing_handler(event: BoomEvent) -> None:
        if event.val == 99:
            runs.append(event.val)
            raise ValueError("boom")

    async def recovery_handler(event: BoomEvent) -> None:
        if event.val == 100:
            runs.append(event.val)

    bus = MessageBus(error_handler=swallowing_error_handler, subscribers=[obs_subscriber])
    bus.register_event_handler(BoomEvent, failing_handler)
    bus.register_event_handler(BoomEvent, recovery_handler)

    # Should not raise any error
    await bus.publish(BoomEvent(val=99))

    # Failing handler runs for 99 (fails, swallowed), recovery runs for 100
    assert runs == [99, 100]

    # Check observability: HandlerFailed must still be dispatched
    assert len(observability_seen) == 1
    assert isinstance(observability_seen[0].error, ValueError)
