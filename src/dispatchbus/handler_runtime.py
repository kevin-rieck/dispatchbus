import asyncio
from collections.abc import Awaitable, Callable
from concurrent.futures import Executor
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from time import perf_counter
from typing import Any

from dispatchbus.callable_runtime import ensure_sync_result, is_async_callable
from dispatchbus.context import EventContext
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
TraceDelivery = Callable[[object], Awaitable[None]]


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
        trace_delivery: TraceDelivery,
        error_handler: ErrorHandler | None = None,
    ) -> None:
        self._executor = executor
        self._trace_delivery = trace_delivery
        self._error_handler = error_handler

    async def invoke(
        self,
        registered_handler: RegisteredHandler,
        message: RuntimeMessage,
        *,
        operation: Operation,
        dispatch_id: str,
    ) -> HandlerOutcome:
        started = perf_counter()
        handler = registered_handler.handler
        name = handler_name(handler)
        payload = payload_of(message)
        metadata = message.metadata
        context = EventContext(metadata)
        await self._trace_delivery(
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

            await self._trace_delivery(
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

        await self._trace_delivery(
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
        if registered_handler.context_style == "keyword":
            call = partial(handler, message, context=context)
        elif registered_handler.context_style == "positional":
            call = partial(handler, message, context)
        else:
            call = partial(handler, message)

        if registered_handler.is_async:
            return await call()
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self._executor, call)
        return ensure_sync_result(result, kind="handler")

    async def _call_error_handler(
        self,
        exc: Exception,
        message: Any,
        handler: Any,
        context: EventContext,
    ) -> None:
        if self._error_handler is None:
            return
        if is_async_callable(self._error_handler):
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
        ensure_sync_result(result, kind="error_handler")
