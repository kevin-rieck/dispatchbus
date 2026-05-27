import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Literal

from dispatchbus.context import EventContext
from dispatchbus.exceptions import BusUsageError, HandlerRegistrationError
from dispatchbus.messages import RuntimeMessage, payload_of
from dispatchbus.observability import (
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
    Operation,
    Subscriber,
    handler_name,
)
from dispatchbus.registry import RegisteredHandler

Handler = Callable[..., Any]
ErrorHandler = Callable[[Exception, Any, Any, EventContext], Awaitable[None] | None]
EventConcurrency = Literal["concurrent", "sequential"]

logger = logging.getLogger("dispatchbus")


def _is_async_callable(value: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(value) or (
        callable(value) and inspect.iscoroutinefunction(value.__call__)
    )


def _raise_if_sync_callable_returned_awaitable(result: Any, *, kind: str) -> Any:
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise BusUsageError(f"sync {kind} returned an awaitable; declare it with async def")
    return result


@dataclass(frozen=True)
class HandlerOutcome:
    result: Any
    emitted_events: tuple[RuntimeMessage, ...]


@dataclass(frozen=True)
class EventDispatchOutcome:
    handler_outcomes: tuple[HandlerOutcome, ...]
    failures: tuple[Exception, ...]


class MessageRuntime:
    def __init__(
        self,
        executor: Executor | None = None,
        *,
        event_concurrency: EventConcurrency = "concurrent",
        error_handler: ErrorHandler | None = None,
    ) -> None:
        if event_concurrency not in {"concurrent", "sequential"}:
            raise HandlerRegistrationError("event_concurrency must be 'concurrent' or 'sequential'")
        self._executor = executor or ThreadPoolExecutor()
        self._owns_executor = executor is None
        self._event_concurrency = event_concurrency
        self._error_handler = error_handler
        self._in_flight: set[asyncio.Task[Any]] = set()

    async def dispatch_command(
        self,
        handler: RegisteredHandler,
        message: Any,
        *,
        dispatch_id: str,
        subscribers: Sequence[Subscriber],
    ) -> HandlerOutcome:
        return await self._call_handler_with_events(
            handler,
            message,
            operation="send",
            dispatch_id=dispatch_id,
            subscribers=subscribers,
        )

    async def dispatch_event(
        self,
        handlers: list[RegisteredHandler],
        message: Any,
        *,
        dispatch_id: str,
        subscribers: Sequence[Subscriber],
        on_outcome: Callable[[HandlerOutcome], Awaitable[None]] | None = None,
    ) -> EventDispatchOutcome:
        if self._event_concurrency == "sequential":
            return await self._dispatch_event_sequential(
                handlers,
                message,
                dispatch_id=dispatch_id,
                subscribers=subscribers,
                on_outcome=on_outcome,
            )
        return await self._dispatch_event_concurrent(
            handlers,
            message,
            dispatch_id=dispatch_id,
            subscribers=subscribers,
            on_outcome=on_outcome,
        )

    async def _dispatch_event_sequential(
        self,
        handlers: list[RegisteredHandler],
        message: Any,
        *,
        dispatch_id: str,
        subscribers: Sequence[Subscriber],
        on_outcome: Callable[[HandlerOutcome], Awaitable[None]] | None,
    ) -> EventDispatchOutcome:
        failures: list[Exception] = []
        outcomes: list[HandlerOutcome] = []
        for handler in handlers:
            try:
                outcome = await self._call_handler_with_events(
                    handler,
                    message,
                    operation="publish",
                    dispatch_id=dispatch_id,
                    subscribers=subscribers,
                )
                outcomes.append(outcome)
                if on_outcome is not None:
                    await on_outcome(outcome)
            except Exception as exc:
                failures.append(exc)
        return EventDispatchOutcome(
            handler_outcomes=tuple(outcomes),
            failures=tuple(failures),
        )

    async def _dispatch_event_concurrent(
        self,
        handlers: list[RegisteredHandler],
        message: Any,
        *,
        dispatch_id: str,
        subscribers: Sequence[Subscriber],
        on_outcome: Callable[[HandlerOutcome], Awaitable[None]] | None,
    ) -> EventDispatchOutcome:
        tasks = [
            asyncio.create_task(
                self._call_handler_with_events(
                    handler,
                    message,
                    operation="publish",
                    dispatch_id=dispatch_id,
                    subscribers=subscribers,
                )
            )
            for handler in handlers
        ]
        for task in tasks:
            self._in_flight.add(task)
            task.add_done_callback(self._in_flight.discard)
        failures: list[Exception] = []
        outcomes: list[HandlerOutcome] = []
        for task in asyncio.as_completed(tasks):
            try:
                outcome = await task
            except Exception as exc:
                failures.append(exc)
            else:
                outcomes.append(outcome)
                if on_outcome is not None:
                    await on_outcome(outcome)
        return EventDispatchOutcome(
            handler_outcomes=tuple(outcomes),
            failures=tuple(failures),
        )

    async def notify_subscribers(self, subscribers: Sequence[Subscriber], event: object) -> None:
        if not subscribers:
            return
        for subscriber in subscribers:
            try:
                await self._call_subscriber(subscriber, event)
            except Exception as exc:
                logger.error(
                    "Error in subscriber %r processing event %r: %s",
                    subscriber,
                    event,
                    exc,
                    exc_info=True,
                )
                continue

    async def _call_handler_with_events(
        self,
        registered_handler: RegisteredHandler,
        message: Any,
        *,
        operation: Operation,
        dispatch_id: str,
        subscribers: Sequence[Subscriber],
    ) -> HandlerOutcome:
        started = perf_counter()
        handler = registered_handler.handler
        name = handler_name(handler)
        payload = payload_of(message)
        metadata = message.metadata
        context = EventContext(metadata)
        await self.notify_subscribers(
            subscribers,
            HandlerStarted(
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation=operation,
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler=handler,
                handler_name=name,
            ),
        )
        try:
            result = await self._call_handler(registered_handler, payload, context)
        except Exception as exc:
            if self._error_handler is not None:
                try:
                    await self._call_error_handler(exc, payload, handler, context)
                    # For now, if it returns normally, we still notify and raise
                    # to satisfy the tests in Task 2 which expect error propagation.
                    # We will implement swallowing in Task 3.
                    await self.notify_subscribers(
                        subscribers,
                        HandlerFailed(
                            message=payload,
                            metadata=metadata,
                            message_type=type(payload),
                            operation=operation,
                            timestamp=datetime.now(),
                            dispatch_id=dispatch_id,
                            handler=handler,
                            handler_name=name,
                            duration_ms=(perf_counter() - started) * 1000,
                            error=exc,
                        ),
                    )
                    raise exc
                except Exception as handler_exc:
                    await self.notify_subscribers(
                        subscribers,
                        HandlerFailed(
                            message=payload,
                            metadata=metadata,
                            message_type=type(payload),
                            operation=operation,
                            timestamp=datetime.now(),
                            dispatch_id=dispatch_id,
                            handler=handler,
                            handler_name=name,
                            duration_ms=(perf_counter() - started) * 1000,
                            error=handler_exc,
                        ),
                    )
                    raise
            else:
                await self.notify_subscribers(
                    subscribers,
                    HandlerFailed(
                        message=payload,
                        metadata=metadata,
                        message_type=type(payload),
                        operation=operation,
                        timestamp=datetime.now(),
                        dispatch_id=dispatch_id,
                        handler=handler,
                        handler_name=name,
                        duration_ms=(perf_counter() - started) * 1000,
                        error=exc,
                    ),
                )
                raise

        await self.notify_subscribers(
            subscribers,
            HandlerFinished(
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation=operation,
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler=handler,
                handler_name=name,
                duration_ms=(perf_counter() - started) * 1000,
            ),
        )
        return HandlerOutcome(result=result, emitted_events=tuple(context.events))

    async def _call_handler(
        self, registered_handler: RegisteredHandler, message: Any, context: EventContext
    ) -> Any:
        handler = registered_handler.handler
        if registered_handler.is_async:
            if registered_handler.context_style == "keyword":
                return await handler(message, context=context)
            if registered_handler.context_style == "positional":
                return await handler(message, context)
            return await handler(message)
        loop = asyncio.get_running_loop()
        if registered_handler.context_style == "keyword":
            result = await loop.run_in_executor(
                self._executor,
                lambda: handler(message, context=context),
            )
            return _raise_if_sync_callable_returned_awaitable(result, kind="handler")
        if registered_handler.context_style == "positional":
            result = await loop.run_in_executor(self._executor, handler, message, context)
            return _raise_if_sync_callable_returned_awaitable(result, kind="handler")
        result = await loop.run_in_executor(self._executor, handler, message)
        return _raise_if_sync_callable_returned_awaitable(result, kind="handler")

    async def _call_error_handler(
        self,
        exc: Exception,
        message: Any,
        handler: Any,
        context: EventContext,
    ) -> None:
        if self._error_handler is None:
            return
        if _is_async_callable(self._error_handler):
            res = self._error_handler(exc, message, handler, context)
            if res is not None:
                await res
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                self._executor,
                self._error_handler,
                exc,
                message,
                handler,
                context,
            )

    async def _call_subscriber(self, subscriber: Subscriber, event: object) -> Any:
        if _is_async_callable(subscriber):
            return await subscriber(event)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self._executor, subscriber, event)
        return _raise_if_sync_callable_returned_awaitable(result, kind="subscriber")

    async def aclose(self) -> None:
        if self._in_flight:
            await asyncio.gather(*list(self._in_flight), return_exceptions=True)
        if self._owns_executor:
            self._executor.shutdown(wait=True)
