import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import Executor
from datetime import datetime
from time import perf_counter
from typing import Any, Literal

from dispatchbus.exceptions import BusDrainingError, EventPublicationError, HandlerRegistrationError
from dispatchbus.lifecycle import BusLifecycle
from dispatchbus.messages import RuntimeMessage, as_runtime_message, message_type_of, payload_of
from dispatchbus.middleware import Middleware, compose_middleware
from dispatchbus.observability import DispatchFinished, DispatchStarted, Subscriber, new_dispatch_id
from dispatchbus.registry import HandlerRegistry, RegisteredHandler
from dispatchbus.runtime import ErrorHandler, HandlerOutcome, MessageRuntime

EventConcurrency = Literal["concurrent", "sequential"]
OutcomeCallback = Callable[[HandlerOutcome], Awaitable[None]]


class DispatchTree:
    """Own and orchestrate the state for accepted command and event dispatch trees."""

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
        self._runtime = MessageRuntime(executor=executor, error_handler=error_handler)
        self._middleware = tuple(middleware or ())
        self._subscribers = list(subscribers or ())
        self._lifecycle = BusLifecycle()
        self._event_concurrency = event_concurrency

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_event_handler(message_type, handler)

    def add_subscriber(self, subscriber: Subscriber) -> None:
        self._subscribers.append(subscriber)

    async def send(self, command: Any) -> Any:
        token = await self._lifecycle.enter_send()
        try:
            return await self._send(command)
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _send(self, command: Any) -> Any:
        runtime_command = as_runtime_message(command)
        payload = payload_of(runtime_command)
        metadata = runtime_command.metadata
        handler = self._registry.get_command_handler(message_type_of(runtime_command))
        dispatch_id = new_dispatch_id()
        started = perf_counter()
        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchStarted(
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation="send",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=1,
            ),
        )

        async def final_handler(message: Any) -> Any:
            outcome = await self._runtime.dispatch_command(
                handler,
                runtime_command,
                dispatch_id=dispatch_id,
                subscribers=self._subscribers,
            )
            await self._publish_events(outcome.emitted_events)
            return outcome.result

        pipeline = compose_middleware(self._middleware, final_handler)
        try:
            result = await pipeline(payload)
        except Exception:
            await self._notify_dispatch_finished(
                payload=payload,
                metadata=metadata,
                operation="send",
                dispatch_id=dispatch_id,
                handler_count=1,
                started=started,
                success=False,
            )
            raise

        await self._notify_dispatch_finished(
            payload=payload,
            metadata=metadata,
            operation="send",
            dispatch_id=dispatch_id,
            handler_count=1,
            started=started,
            success=True,
        )
        return result

    async def publish(self, event: Any) -> None:
        token = await self._lifecycle.enter_publish()
        try:
            await self._publish_events((as_runtime_message(event),))
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _publish_events(self, events: Sequence[RuntimeMessage]) -> None:
        if self._event_concurrency == "sequential":
            await self._publish_events_sequential(events)
        else:
            await self._publish_events_concurrent(events)

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
            except BusDrainingError:
                raise
            except Exception as exc:
                failures.append(exc)
            pending.extend(follow_ups)

        if failures:
            raise EventPublicationError(failures)

    async def _publish_events_concurrent(self, events: Sequence[RuntimeMessage]) -> None:
        tasks = [asyncio.create_task(self._publish_concurrent_event(event)) for event in events]
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
                asyncio.create_task(self._publish_concurrent_event(emitted_event))
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
        payload = payload_of(runtime_event)
        metadata = runtime_event.metadata
        handlers = self._registry.get_event_handlers(message_type_of(runtime_event))
        dispatch_id = new_dispatch_id()
        started = perf_counter()
        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchStarted(
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation="publish",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=len(handlers),
            ),
        )

        async def final_handler(message: Any) -> None:
            failures = await self._dispatch_event_handlers(
                handlers,
                runtime_event,
                dispatch_id=dispatch_id,
                on_outcome=on_outcome,
            )
            if failures:
                raise EventPublicationError(failures)

        pipeline = compose_middleware(self._middleware, final_handler)
        try:
            await pipeline(payload)
        except Exception:
            await self._notify_dispatch_finished(
                payload=payload,
                metadata=metadata,
                operation="publish",
                dispatch_id=dispatch_id,
                handler_count=len(handlers),
                started=started,
                success=False,
            )
            raise

        await self._notify_dispatch_finished(
            payload=payload,
            metadata=metadata,
            operation="publish",
            dispatch_id=dispatch_id,
            handler_count=len(handlers),
            started=started,
            success=True,
        )

    async def _dispatch_event_handlers(
        self,
        handlers: Sequence[RegisteredHandler],
        event: RuntimeMessage,
        *,
        dispatch_id: str,
        on_outcome: OutcomeCallback,
    ) -> list[Exception]:
        if self._event_concurrency == "sequential":
            return await self._dispatch_event_handlers_sequential(
                handlers,
                event,
                dispatch_id=dispatch_id,
                on_outcome=on_outcome,
            )
        return await self._dispatch_event_handlers_concurrent(
            handlers,
            event,
            dispatch_id=dispatch_id,
            on_outcome=on_outcome,
        )

    async def _dispatch_event_handlers_sequential(
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
                outcome = await self._runtime.dispatch_event_handler(
                    handler,
                    event,
                    dispatch_id=dispatch_id,
                    subscribers=self._subscribers,
                )
                await on_outcome(outcome)
            except Exception as exc:
                failures.append(exc)
        return failures

    async def _dispatch_event_handlers_concurrent(
        self,
        handlers: Sequence[RegisteredHandler],
        event: RuntimeMessage,
        *,
        dispatch_id: str,
        on_outcome: OutcomeCallback,
    ) -> list[Exception]:
        tasks = [
            asyncio.create_task(
                self._runtime.dispatch_event_handler(
                    handler,
                    event,
                    dispatch_id=dispatch_id,
                    subscribers=self._subscribers,
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

    async def _notify_dispatch_finished(
        self,
        *,
        payload: Any,
        metadata: Any,
        operation: Literal["send", "publish"],
        dispatch_id: str,
        handler_count: int,
        started: float,
        success: bool,
    ) -> None:
        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchFinished(
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation=operation,
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=handler_count,
                duration_ms=(perf_counter() - started) * 1000,
                success=success,
            ),
        )

    async def aclose(self) -> None:
        await self._lifecycle.begin_close()
        await self._runtime.aclose()
        await self._lifecycle.finish_close()
