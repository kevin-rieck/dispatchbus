import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Literal, TypeVar

from dispatchbus.callable_runtime import ensure_sync_result, is_async_callable
from dispatchbus.exceptions import BusDrainingError, EventPublicationError, HandlerRegistrationError
from dispatchbus.handler_runtime import ErrorHandler, HandlerOutcome, HandlerRuntime
from dispatchbus.lifecycle import BusLifecycle
from dispatchbus.messages import RuntimeMessage, as_runtime_message, message_type_of, payload_of
from dispatchbus.middleware import Middleware, compose_middleware
from dispatchbus.observability import (
    DispatchFinished,
    DispatchStarted,
    Operation,
    Subscriber,
    new_dispatch_id,
)
from dispatchbus.registry import HandlerRegistry, RegisteredHandler

EventConcurrency = Literal["concurrent", "sequential"]
OutcomeCallback = Callable[[HandlerOutcome], Awaitable[None]]
TaskResult = TypeVar("TaskResult")
logger = logging.getLogger("dispatchbus")


@dataclass(frozen=True)
class _DispatchTraceState:
    payload: Any
    metadata: Any
    operation: Operation
    dispatch_id: str
    handler_count: int
    started: float


class DispatchTree:
    """Coordinate accepted dispatch trees and own their shared runtime state."""

    def __init__(
        self,
        middleware: Sequence[Middleware] | None = None,
        *,
        event_concurrency: EventConcurrency = "concurrent",
        subscribers: Sequence[Subscriber] | None = None,
        executor: Executor | None = None,
        error_handler: ErrorHandler | None = None,
    ) -> None:
        if event_concurrency not in {"concurrent", "sequential"}:
            raise HandlerRegistrationError("event_concurrency must be 'concurrent' or 'sequential'")
        self._registry = HandlerRegistry()
        self._executor = executor if executor is not None else ThreadPoolExecutor()
        self._owns_executor = executor is None
        self._middleware = tuple(middleware or ())
        self._subscribers = list(subscribers or ())
        self._handler_runtime = HandlerRuntime(
            self._executor,
            trace_delivery=self._deliver_trace,
            error_handler=error_handler,
        )
        self._lifecycle = BusLifecycle()
        self._publish_events_impl: Callable[[Sequence[RuntimeMessage]], Awaitable[None]]
        self._dispatch_handlers_impl: Callable[..., Awaitable[list[Exception]]]
        if event_concurrency == "sequential":
            self._publish_events_impl = self._publish_events_sequential
            self._dispatch_handlers_impl = self._dispatch_handlers_sequential
        else:
            self._publish_events_impl = self._publish_events_concurrent
            self._dispatch_handlers_impl = self._dispatch_handlers_concurrent
        self._event_tasks: set[asyncio.Task[Any]] = set()

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_event_handler(message_type, handler)

    def add_subscriber(self, subscriber: Subscriber) -> None:
        self._subscribers.append(subscriber)

    async def send(self, command: Any) -> Any:
        async with self._lifecycle.admit_command():
            return await self._send(command)

    async def _send(self, command: Any) -> Any:
        runtime_command = as_runtime_message(command)
        handler = self._registry.get_command_handler(message_type_of(runtime_command))
        dispatch_id = new_dispatch_id()

        async def final_handler(message: Any) -> Any:
            outcome = await self._handler_runtime.invoke(
                handler,
                runtime_command,
                operation="send",
                dispatch_id=dispatch_id,
            )
            await self._publish_events_impl(outcome.emitted_events)
            return outcome.result

        return await self._run_dispatch(
            runtime_command,
            operation="send",
            dispatch_id=dispatch_id,
            handler_count=1,
            final_handler=final_handler,
        )

    async def publish(self, event: Any) -> None:
        token = await self._lifecycle.enter_publish()
        try:
            await self._publish_events_impl((as_runtime_message(event),))
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _publish_events_sequential(self, events: Sequence[RuntimeMessage]) -> None:
        pending = deque(events)
        failures: list[Exception] = []

        while pending:
            event = pending.popleft()
            follow_ups: list[RuntimeMessage] = []

            async def collect(
                outcome: HandlerOutcome, target: list[RuntimeMessage] = follow_ups
            ) -> None:
                target.extend(outcome.emitted_events)

            try:
                await self._publish_admitted_event(event, on_outcome=collect)
            except EventPublicationError as exc:
                failures.extend(exc.failures)
                pending.extend(follow_ups)
            except BusDrainingError:
                raise
            except Exception as exc:
                failures.append(exc)
            else:
                pending.extend(follow_ups)

        if failures:
            raise EventPublicationError(failures)

    async def _publish_events_concurrent(self, events: Sequence[RuntimeMessage]) -> None:
        tasks = [self._create_event_task(self._publish_concurrent_event(event)) for event in events]
        failures = await self._collect_publication_failures(tasks)
        for failure in failures:
            if isinstance(failure, BusDrainingError):
                raise failure
        if failures:
            raise EventPublicationError(failures)

    async def _publish_concurrent_event(self, event: RuntimeMessage) -> None:
        follow_up_tasks: list[asyncio.Task[None]] = []

        async def schedule(outcome: HandlerOutcome) -> None:
            follow_up_tasks.extend(
                self._create_event_task(self._publish_concurrent_event(emitted_event))
                for emitted_event in outcome.emitted_events
            )

        direct_failures: list[Exception] = []
        try:
            await self._publish_admitted_event(event, on_outcome=schedule)
        except EventPublicationError as exc:
            direct_failures.extend(exc.failures)
        except BusDrainingError:
            raise
        except Exception as exc:
            direct_failures.append(exc)

        follow_up_failures = await self._collect_publication_failures(follow_up_tasks)
        failures = direct_failures + follow_up_failures
        if failures:
            raise EventPublicationError(failures)

    async def _collect_publication_failures(
        self, tasks: Sequence[asyncio.Task[None]]
    ) -> list[Exception]:
        failures: list[Exception] = []
        for task in asyncio.as_completed(tasks):
            try:
                await task
            except EventPublicationError as exc:
                failures.extend(exc.failures)
            except Exception as exc:
                failures.append(exc)
        return failures

    async def _publish_admitted_event(
        self,
        event: RuntimeMessage,
        *,
        on_outcome: OutcomeCallback,
    ) -> None:
        token = await self._lifecycle.enter_publish()
        try:
            await self._publish_one_event(event, on_outcome=on_outcome)
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _publish_one_event(
        self,
        runtime_event: RuntimeMessage,
        *,
        on_outcome: OutcomeCallback,
    ) -> None:
        handlers = self._registry.get_event_handlers(message_type_of(runtime_event))
        dispatch_id = new_dispatch_id()

        async def final_handler(message: Any) -> None:
            failures = await self._dispatch_handlers_impl(
                handlers,
                runtime_event,
                dispatch_id=dispatch_id,
                on_outcome=on_outcome,
            )
            if failures:
                raise EventPublicationError(failures)

        await self._run_dispatch(
            runtime_event,
            operation="publish",
            dispatch_id=dispatch_id,
            handler_count=len(handlers),
            final_handler=final_handler,
        )

    async def _dispatch_handlers_sequential(
        self,
        handlers: Sequence[RegisteredHandler],
        event: RuntimeMessage,
        *,
        dispatch_id: str,
        on_outcome: OutcomeCallback,
    ) -> list[Exception]:
        failures: list[Exception] = []
        for handler in handlers:
            try:
                outcome = await self._handler_runtime.invoke(
                    handler,
                    event,
                    operation="publish",
                    dispatch_id=dispatch_id,
                )
                await on_outcome(outcome)
            except Exception as exc:
                failures.append(exc)
        return failures

    async def _dispatch_handlers_concurrent(
        self,
        handlers: Sequence[RegisteredHandler],
        event: RuntimeMessage,
        *,
        dispatch_id: str,
        on_outcome: OutcomeCallback,
    ) -> list[Exception]:
        tasks = [
            self._create_event_task(
                self._handler_runtime.invoke(
                    handler,
                    event,
                    operation="publish",
                    dispatch_id=dispatch_id,
                )
            )
            for handler in handlers
        ]
        failures: list[Exception] = []
        for task in asyncio.as_completed(tasks):
            try:
                outcome = await task
                await on_outcome(outcome)
            except Exception as exc:
                failures.append(exc)
        return failures

    async def _run_dispatch(
        self,
        message: RuntimeMessage,
        *,
        operation: Operation,
        dispatch_id: str,
        handler_count: int,
        final_handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        trace = _DispatchTraceState(
            payload=payload_of(message),
            metadata=message.metadata,
            operation=operation,
            dispatch_id=dispatch_id,
            handler_count=handler_count,
            started=perf_counter(),
        )
        await self._deliver_trace(
            DispatchStarted(
                message=trace.payload,
                metadata=trace.metadata,
                message_type=type(trace.payload),
                operation=trace.operation,
                timestamp=datetime.now(),
                dispatch_id=trace.dispatch_id,
                handler_count=trace.handler_count,
            )
        )

        pipeline = compose_middleware(self._middleware, final_handler)
        try:
            result = await pipeline(trace.payload)
        except Exception:
            await self._notify_dispatch_finished(trace, success=False)
            raise

        await self._notify_dispatch_finished(trace, success=True)
        return result

    async def _notify_dispatch_finished(self, trace: _DispatchTraceState, *, success: bool) -> None:
        await self._deliver_trace(
            DispatchFinished(
                message=trace.payload,
                metadata=trace.metadata,
                message_type=type(trace.payload),
                operation=trace.operation,
                timestamp=datetime.now(),
                dispatch_id=trace.dispatch_id,
                handler_count=trace.handler_count,
                duration_ms=(perf_counter() - trace.started) * 1000,
                success=success,
            )
        )

    async def _deliver_trace(self, fact: object) -> None:
        for subscriber in self._subscribers:
            try:
                await self._call_subscriber(subscriber, fact)
            except Exception as exc:
                logger.error(
                    "Error in subscriber %r processing dispatch trace fact %r: %s",
                    subscriber,
                    fact,
                    exc,
                    exc_info=True,
                )

    async def _call_subscriber(self, subscriber: Subscriber, fact: object) -> Any:
        if is_async_callable(subscriber):
            return await subscriber(fact)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self._executor, subscriber, fact)
        return ensure_sync_result(result, kind="subscriber")

    def _create_event_task(
        self, coroutine: Coroutine[Any, Any, TaskResult]
    ) -> asyncio.Task[TaskResult]:
        task = asyncio.create_task(coroutine)
        self._event_tasks.add(task)
        task.add_done_callback(self._event_tasks.discard)
        return task

    async def _drain(self) -> None:
        while self._event_tasks:
            await asyncio.gather(*tuple(self._event_tasks), return_exceptions=True)

    async def aclose(self) -> None:
        await self._lifecycle.begin_close()
        await self._drain()
        if self._owns_executor:
            self._executor.shutdown(wait=True)
        await self._lifecycle.finish_close()
