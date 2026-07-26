from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pytest

from dispatchbus.context import EventContext
from dispatchbus.exceptions import BusUsageError
from dispatchbus.handler_runtime import HandlerRuntime
from dispatchbus.messages import CommandBase, EventBase, as_runtime_message
from dispatchbus.registry import RegisteredHandler


async def ignore_trace(event: object) -> None:
    return None


@dataclass(frozen=True)
class DoWork(CommandBase):
    message_name = "test.do_work"

    value: int


@dataclass(frozen=True)
class WorkFinished(EventBase):
    message_name = "test.work_finished"

    value: int


@pytest.mark.asyncio
async def test_invoke_collects_result_and_follow_up_events() -> None:
    async def handler(command: DoWork, context: EventContext) -> int:
        context.emit(WorkFinished(value=command.value))
        return command.value * 2

    registered = RegisteredHandler(handler=handler, is_async=True, context_style="positional")

    with ThreadPoolExecutor(max_workers=1) as executor:
        runtime = HandlerRuntime(executor, trace_delivery=ignore_trace)
        outcome = await runtime.invoke(
            registered,
            as_runtime_message(DoWork(value=3)),
            operation="send",
            dispatch_id="dispatch-id",
        )

    assert outcome.result == 6
    assert [event.payload for event in outcome.emitted_events] == [WorkFinished(value=3)]


@pytest.mark.asyncio
async def test_invoke_returns_recovery_events_when_error_handler_swallows_failure() -> None:
    async def handler(command: DoWork) -> None:
        raise ValueError(f"failed {command.value}")

    async def error_handler(
        exc: Exception,
        message: DoWork,
        failed_handler: object,
        context: EventContext,
    ) -> None:
        assert str(exc) == "failed 4"
        assert failed_handler is handler
        context.emit(WorkFinished(value=message.value))

    registered = RegisteredHandler(handler=handler, is_async=True, context_style="none")

    with ThreadPoolExecutor(max_workers=1) as executor:
        runtime = HandlerRuntime(
            executor,
            trace_delivery=ignore_trace,
            error_handler=error_handler,
        )
        outcome = await runtime.invoke(
            registered,
            as_runtime_message(DoWork(value=4)),
            operation="send",
            dispatch_id="dispatch-id",
        )

    assert outcome.result is None
    assert [event.payload for event in outcome.emitted_events] == [WorkFinished(value=4)]


@pytest.mark.asyncio
async def test_invoke_rejects_sync_error_handler_returning_awaitable() -> None:
    async def handler(command: DoWork) -> None:
        raise ValueError("handler failed")

    async def inner() -> None:
        return None

    def error_handler(
        exc: Exception,
        message: DoWork,
        failed_handler: object,
        context: EventContext,
    ):
        return inner()

    registered = RegisteredHandler(handler=handler, is_async=True, context_style="none")

    with ThreadPoolExecutor(max_workers=1) as executor:
        runtime = HandlerRuntime(
            executor,
            trace_delivery=ignore_trace,
            error_handler=error_handler,
        )

        with pytest.raises(BusUsageError, match="sync error_handler returned an awaitable"):
            await runtime.invoke(
                registered,
                as_runtime_message(DoWork(value=5)),
                operation="send",
                dispatch_id="dispatch-id",
            )


@pytest.mark.asyncio
async def test_invoke_rejects_sync_handler_returning_awaitable() -> None:
    async def inner(command: DoWork) -> int:
        return command.value

    def handler(command: DoWork):
        return inner(command)

    with ThreadPoolExecutor(max_workers=1) as executor:
        runtime = HandlerRuntime(executor, trace_delivery=ignore_trace)
        registered = RegisteredHandler(handler=handler, is_async=False, context_style="none")

        with pytest.raises(BusUsageError, match="sync handler returned an awaitable"):
            await runtime.invoke(
                registered,
                as_runtime_message(DoWork(value=5)),
                operation="send",
                dispatch_id="dispatch-id",
            )
