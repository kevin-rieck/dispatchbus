import asyncio
import inspect
from collections.abc import Callable, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from datetime import datetime
from time import perf_counter
from typing import Any, Literal

from dispatchr.exceptions import EventPublicationError, HandlerRegistrationError
from dispatchr.observability import (
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
    Operation,
    Subscriber,
    handler_name,
)

Handler = Callable[[Any], Any]
EventConcurrency = Literal["concurrent", "sequential"]


def _is_async_callable(value: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(value) or (
        callable(value) and inspect.iscoroutinefunction(value.__call__)
    )


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
    ) -> Any:
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
    ) -> None:
        if self._event_concurrency == "sequential":
            failures: list[Exception] = []
            for handler in handlers:
                try:
                    await self._call_handler_with_events(
                        handler,
                        message,
                        operation="publish",
                        dispatch_id=dispatch_id,
                        subscribers=subscribers,
                    )
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise EventPublicationError(failures)
            return

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
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            raise EventPublicationError(failures)

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
    ) -> Any:
        started = perf_counter()
        name = handler_name(handler)
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
            result = await self._call_handler(handler, message)
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
        return result

    async def _call_handler(self, handler: Handler, message: Any) -> Any:
        if _is_async_callable(handler):
            return await handler(message)
        loop = asyncio.get_running_loop()
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
