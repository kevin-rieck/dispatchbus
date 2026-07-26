import asyncio
import inspect
from collections.abc import Awaitable, Callable
from concurrent.futures import Executor
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any

from dispatchbus.context import EventContext
from dispatchbus.exceptions import BusUsageError
from dispatchbus.messages import RuntimeMessage, payload_of
from dispatchbus.observability import (
    HandlerFailed,
    HandlerFinished,
    HandlerStarted,
    Operation,
    handler_name,
)
from dispatchbus.registry import RegisteredHandler

ErrorHandler = Callable[[Exception, Any, Any, EventContext], Awaitable[None] | None]
TraceNotifier = Callable[[object], Awaitable[None]]


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


class HandlerRuntime:
    """Invoke one registered handler and collect its outcome."""

    def __init__(
        self,
        executor: Executor,
        *,
        error_handler: ErrorHandler | None = None,
    ) -> None:
        self._executor = executor
        self._error_handler = error_handler

    async def invoke(
        self,
        registered_handler: RegisteredHandler,
        message: RuntimeMessage,
        *,
        operation: Operation,
        dispatch_id: str,
        notify: TraceNotifier,
    ) -> HandlerOutcome:
        started = perf_counter()
        handler = registered_handler.handler
        name = handler_name(handler)
        payload = payload_of(message)
        metadata = message.metadata
        context = EventContext(metadata)
        await notify(
            HandlerStarted(
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation=operation,
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler=handler,
                handler_name=name,
            )
        )
        try:
            result = await self._call_handler(registered_handler, payload, context)
        except Exception as exc:
            failed_duration_ms = (perf_counter() - started) * 1000
            final_error = exc
            swallowed = False
            if self._error_handler is not None:
                try:
                    await self._call_error_handler(exc, payload, handler, context)
                    swallowed = True
                except Exception as handler_exc:
                    final_error = handler_exc

            await notify(
                HandlerFailed(
                    message=payload,
                    metadata=metadata,
                    message_type=type(payload),
                    operation=operation,
                    timestamp=datetime.now(),
                    dispatch_id=dispatch_id,
                    handler=handler,
                    handler_name=name,
                    duration_ms=failed_duration_ms,
                    error=final_error,
                )
            )

            if swallowed:
                return HandlerOutcome(result=None, emitted_events=tuple(context.events))

            if final_error is exc:
                raise
            raise final_error from exc

        await notify(
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
            )
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
            result = self._error_handler(exc, message, handler, context)
            if result is not None:
                await result
            return
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self._executor,
            self._error_handler,
            exc,
            message,
            handler,
            context,
        )
        _raise_if_sync_callable_returned_awaitable(result, kind="error_handler")
