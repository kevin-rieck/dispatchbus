from collections.abc import Sequence
from concurrent.futures import Executor
from datetime import datetime
from time import perf_counter
from typing import Any

from dispatchbus.event_publisher import EventPublisher
from dispatchbus.exceptions import EventPublicationError
from dispatchbus.lifecycle import BusLifecycle
from dispatchbus.messages import as_runtime_message, message_type_of, payload_of
from dispatchbus.middleware import Middleware, compose_middleware
from dispatchbus.observability import DispatchFinished, DispatchStarted, Subscriber, new_dispatch_id
from dispatchbus.registry import HandlerRegistry
from dispatchbus.runtime import ErrorHandler, EventConcurrency, MessageRuntime


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
        self._registry = HandlerRegistry()
        self._runtime = MessageRuntime(
            event_concurrency=event_concurrency,
            executor=executor,
            error_handler=error_handler,
        )
        self._middleware = tuple(middleware or ())
        self._subscribers = list(subscribers or ())
        self._lifecycle = BusLifecycle()
        self._event_publisher = EventPublisher(
            registry=self._registry,
            runtime=self._runtime,
            middleware=self._middleware,
            subscribers=self._subscribers,
        )

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_event_handler(message_type, handler)

    def add_subscriber(self, subscriber: Subscriber) -> None:
        self._subscribers.append(subscriber)
        self._event_publisher.add_subscriber(subscriber)

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
            failures: list[Exception] = []
            for emitted_event in outcome.emitted_events:
                try:
                    await self.publish(emitted_event)
                except EventPublicationError as exc:
                    failures.extend(exc.failures)
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise EventPublicationError(failures)
            return outcome.result

        pipeline = compose_middleware(self._middleware, final_handler)
        try:
            result = await pipeline(payload)
        except Exception:
            await self._notify_dispatch_finished(
                payload=payload,
                metadata=metadata,
                dispatch_id=dispatch_id,
                started=started,
                success=False,
            )
            raise

        await self._notify_dispatch_finished(
            payload=payload,
            metadata=metadata,
            dispatch_id=dispatch_id,
            started=started,
            success=True,
        )
        return result

    async def _notify_dispatch_finished(
        self,
        *,
        payload: Any,
        metadata: Any,
        dispatch_id: str,
        started: float,
        success: bool,
    ) -> None:
        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchFinished(
                message=payload,
                metadata=metadata,
                message_type=type(payload),
                operation="send",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=1,
                duration_ms=(perf_counter() - started) * 1000,
                success=success,
            ),
        )

    async def publish(self, event: Any) -> None:
        token = await self._lifecycle.enter_publish()
        try:
            await self._event_publisher.publish(event)
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def aclose(self) -> None:
        await self._lifecycle.begin_close()
        await self._runtime.aclose()
        await self._lifecycle.finish_close()
