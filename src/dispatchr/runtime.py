import asyncio
import inspect
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Literal

from dispatchr.context import EventContext
from dispatchr.exceptions import HandlerRegistrationError
from dispatchr.observability import (
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
    Operation,
    Subscriber,
    handler_name,
)

Handler = Callable[..., Any]
EventConcurrency = Literal["concurrent", "sequential"]


@dataclass(frozen=True)
class HandlerOutcome:
    result: Any
    emitted_events: tuple[object, ...]


@dataclass(frozen=True)
class EventDispatchOutcome:
    handler_outcomes: tuple[HandlerOutcome, ...]
    failures: tuple[Exception, ...]


def _is_async_callable(value: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(value) or (
        callable(value) and inspect.iscoroutinefunction(value.__call__)
    )


def _callable_signature(value: Callable[..., Any]) -> inspect.Signature:
    return inspect.signature(value)


def _context_parameter(value: Callable[..., Any]) -> inspect.Parameter | None:
    parameters = list(_callable_signature(value).parameters.values())
    positional = [
        parameter
        for parameter in parameters
        if parameter.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    if (
        len(positional) >= 2
        and positional[1].name == "context"
        and positional[1].default is inspect.Parameter.empty
    ):
        return positional[1]
    for parameter in parameters:
        if (
            parameter.kind is inspect.Parameter.KEYWORD_ONLY
            and parameter.name == "context"
            and parameter.default is inspect.Parameter.empty
        ):
            return parameter
    return None


def _accepts_event_context(value: Callable[..., Any]) -> bool:
    return _context_parameter(value) is not None


class MessageRuntime:
    def __init__(
        self,
        executor: Executor | None = None,
        *,
        event_concurrency: EventConcurrency = "concurrent",
    ) -> None:
        if event_concurrency not in {"concurrent", "sequential"}:
            raise HandlerRegistrationError("event_concurrency must be 'concurrent' or 'sequential'")
        self._executor = executor or ThreadPoolExecutor()
        self._owns_executor = executor is None
        self._event_concurrency = event_concurrency
        self._in_flight: set[asyncio.Task[Any]] = set()

    async def dispatch_command(
        self,
        handler: Handler,
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
        handlers: list[Handler],
        message: Any,
        *,
        dispatch_id: str,
        subscribers: Sequence[Subscriber],
        on_outcome: Callable[[HandlerOutcome], Awaitable[None]] | None = None,
    ) -> EventDispatchOutcome:
        if self._event_concurrency == "sequential":
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
        concurrent_failures: list[Exception] = []
        concurrent_outcomes: list[HandlerOutcome] = []
        for task in asyncio.as_completed(tasks):
            try:
                result = await task
            except Exception as exc:
                concurrent_failures.append(exc)
            else:
                concurrent_outcomes.append(result)
                if on_outcome is not None:
                    await on_outcome(result)
        return EventDispatchOutcome(
            handler_outcomes=tuple(concurrent_outcomes),
            failures=tuple(concurrent_failures),
        )

    async def notify_subscribers(self, subscribers: Sequence[Subscriber], event: object) -> None:
        if not subscribers:
            return
        for subscriber in subscribers:
            try:
                await self._call_subscriber(subscriber, event)
            except Exception:
                continue

    async def _call_handler_with_events(
        self,
        handler: Handler,
        message: Any,
        *,
        operation: Operation,
        dispatch_id: str,
        subscribers: Sequence[Subscriber],
    ) -> HandlerOutcome:
        started = perf_counter()
        name = handler_name(handler)
        context = EventContext()
        await self.notify_subscribers(
            subscribers,
            HandlerStarted(
                message=message,
                message_type=type(message),
                operation=operation,
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler=handler,
                handler_name=name,
            ),
        )
        try:
            result = await self._call_handler(handler, message, context)
        except Exception as exc:
            await self.notify_subscribers(
                subscribers,
                HandlerFailed(
                    message=message,
                    message_type=type(message),
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
                message=message,
                message_type=type(message),
                operation=operation,
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler=handler,
                handler_name=name,
                duration_ms=(perf_counter() - started) * 1000,
            ),
        )
        return HandlerOutcome(result=result, emitted_events=tuple(context.events))

    async def _call_handler(self, handler: Handler, message: Any, context: EventContext) -> Any:
        context_parameter = _context_parameter(handler)
        if _is_async_callable(handler):
            if context_parameter is not None:
                if context_parameter.kind is inspect.Parameter.KEYWORD_ONLY:
                    return await handler(message, context=context)
                return await handler(message, context)
            return await handler(message)
        loop = asyncio.get_running_loop()
        if context_parameter is not None:
            if context_parameter.kind is inspect.Parameter.KEYWORD_ONLY:
                return await loop.run_in_executor(
                    self._executor,
                    lambda: handler(message, context=context),
                )
            return await loop.run_in_executor(self._executor, handler, message, context)
        return await loop.run_in_executor(self._executor, handler, message)

    async def _call_subscriber(self, subscriber: Subscriber, event: object) -> Any:
        if _is_async_callable(subscriber):
            return await subscriber(event)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, subscriber, event)

    async def aclose(self) -> None:
        if self._in_flight:
            await asyncio.gather(*list(self._in_flight), return_exceptions=True)
        if self._owns_executor:
            self._executor.shutdown(wait=True)
