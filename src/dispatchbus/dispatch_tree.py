import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from datetime import datetime
from time import perf_counter
from typing import Any, Literal

from dispatchbus.callable_runtime import ensure_sync_result, is_async_callable
from dispatchbus.event_publisher import EventPublisher
from dispatchbus.exceptions import EventPublicationError, HandlerRegistrationError
from dispatchbus.handler_runtime import ErrorHandler, HandlerRuntime
from dispatchbus.lifecycle import BusLifecycle
from dispatchbus.messages import RuntimeMessage, as_runtime_message, message_type_of, payload_of
from dispatchbus.middleware import Middleware, compose_middleware
from dispatchbus.observability import DispatchFinished, DispatchStarted, Subscriber, new_dispatch_id
from dispatchbus.registry import HandlerRegistry

EventConcurrency = Literal["concurrent", "sequential"]
logger = logging.getLogger("dispatchbus")


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
        self._event_publisher = EventPublisher(
            registry=self._registry,
            handler_runtime=self._handler_runtime,
            lifecycle=self._lifecycle,
            event_concurrency=event_concurrency,
            run_dispatch=self._run_dispatch,
        )

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
        handler = self._registry.get_command_handler(message_type_of(runtime_command))
        dispatch_id = new_dispatch_id()

        async def final_handler(message: Any) -> Any:
            outcome = await self._handler_runtime.invoke(
                handler,
                runtime_command,
                operation="send",
                dispatch_id=dispatch_id,
            )
            failures: list[Exception] = []
            for emitted_event in outcome.emitted_events:
                try:
                    await self._event_publisher.publish_events((emitted_event,))
                except EventPublicationError as exc:
                    failures.extend(exc.failures)
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise EventPublicationError(failures)
            return outcome.result

        return await self._run_dispatch(
            runtime_command,
            operation="send",
            dispatch_id=dispatch_id,
            handler_count=1,
            final_handler=final_handler,
        )

    async def publish(self, event: Any) -> None:
        await self._event_publisher.publish(event)

    async def _run_dispatch(
        self,
        message: RuntimeMessage,
        *,
        operation: Literal["send", "publish"],
        dispatch_id: str,
        handler_count: int,
        final_handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        payload = payload_of(message)
        metadata = message.metadata
        started = perf_counter()
        await self._deliver_trace(
            DispatchStarted(
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation=operation,
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=handler_count,
            )
        )

        pipeline = compose_middleware(self._middleware, final_handler)
        try:
            result = await pipeline(payload)
        except Exception:
            await self._notify_dispatch_finished(
                payload=payload,
                metadata=metadata,
                operation=operation,
                dispatch_id=dispatch_id,
                handler_count=handler_count,
                started=started,
                success=False,
            )
            raise

        await self._notify_dispatch_finished(
            payload=payload,
            metadata=metadata,
            operation=operation,
            dispatch_id=dispatch_id,
            handler_count=handler_count,
            started=started,
            success=True,
        )
        return result

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
        await self._deliver_trace(
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
            )
        )

    async def _deliver_trace(self, event: object) -> None:
        for subscriber in self._subscribers:
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

    async def _call_subscriber(self, subscriber: Subscriber, event: object) -> Any:
        if is_async_callable(subscriber):
            return await subscriber(event)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self._executor, subscriber, event)
        return ensure_sync_result(result, kind="subscriber")

    async def aclose(self) -> None:
        await self._lifecycle.begin_close()
        await self._event_publisher.drain()
        if self._owns_executor:
            self._executor.shutdown(wait=True)
        await self._lifecycle.finish_close()
