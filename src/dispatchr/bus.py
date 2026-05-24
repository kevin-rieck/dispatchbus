import asyncio
import threading
from collections import deque
from collections.abc import Sequence
from concurrent.futures import Future
from datetime import datetime
from time import perf_counter
from typing import Any

from dispatchr.exceptions import BusUsageError, EventPublicationError
from dispatchr.lifecycle import BusLifecycle
from dispatchr.middleware import Middleware, compose_middleware
from dispatchr.observability import DispatchFinished, DispatchStarted, Subscriber, new_dispatch_id
from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import EventConcurrency, MessageRuntime
from dispatchr.sync_bridge import SyncBridge


class MessageBus:
    def __init__(
        self,
        middleware: Sequence[Middleware] | None = None,
        *,
        event_concurrency: EventConcurrency = "concurrent",
        subscribers: Sequence[Subscriber] | None = None,
    ) -> None:
        self._registry = HandlerRegistry()
        self._runtime = MessageRuntime(event_concurrency=event_concurrency)
        self._middleware = list(middleware or [])
        self._subscribers = list(subscribers or [])
        self._lifecycle = BusLifecycle()
        self._sync_bridge = SyncBridge()

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_event_handler(message_type, handler)

    def add_subscriber(self, subscriber: Subscriber) -> None:
        self._subscribers.append(subscriber)

    async def send(self, command: Any) -> Any:
        token = await self._lifecycle.enter_send()
        try:
            return await self._send_impl(command)
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _send_impl(self, command: Any) -> Any:
        handler = self._registry.get_command_handler(type(command))
        dispatch_id = new_dispatch_id()
        started = perf_counter()
        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchStarted(
                message=command,
                message_type=type(command),
                operation="send",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=1,
            ),
        )

        async def final_handler(message: Any) -> Any:
            outcome = await self._runtime.dispatch_command(
                handler,
                message,
                dispatch_id=dispatch_id,
                subscribers=self._subscribers,
            )
            failures: list[Exception] = []
            for emitted_event in outcome.emitted_events:
                try:
                    await self._publish_impl(emitted_event)
                except EventPublicationError as exc:
                    failures.extend(exc.failures)
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise EventPublicationError(failures)
            return outcome.result

        pipeline = compose_middleware(self._middleware, final_handler)
        try:
            result = await pipeline(command)
        except Exception:
            await self._runtime.notify_subscribers(
                self._subscribers,
                DispatchFinished(
                    message=command,
                    message_type=type(command),
                    operation="send",
                    timestamp=datetime.now(),
                    dispatch_id=dispatch_id,
                    handler_count=1,
                    duration_ms=(perf_counter() - started) * 1000,
                    success=False,
                ),
            )
            raise

        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchFinished(
                message=command,
                message_type=type(command),
                operation="send",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=1,
                duration_ms=(perf_counter() - started) * 1000,
                success=True,
            ),
        )
        return result

    async def publish(self, event: Any) -> None:
        token = await self._lifecycle.enter_publish()
        try:
            await self._publish_impl(event)
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _publish_impl(self, event: Any) -> None:
        pending_events = deque([event])
        failures: list[Exception] = []

        while pending_events:
            current_event = pending_events.popleft()
            try:
                emitted_events, event_failures = await self._publish_one_event(current_event)
            except EventPublicationError as exc:
                failures.extend(exc.failures)
            except Exception as exc:
                failures.append(exc)
            else:
                pending_events.extend(emitted_events)
                failures.extend(event_failures)

        if failures:
            raise EventPublicationError(failures)

    async def _publish_one_event(self, event: Any) -> tuple[list[Any], list[Exception]]:
        handlers = self._registry.get_event_handlers(type(event))
        dispatch_id = new_dispatch_id()
        started = perf_counter()
        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchStarted(
                message=event,
                message_type=type(event),
                operation="publish",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=len(handlers),
            ),
        )

        failures: list[Exception] = []

        async def final_handler(message: Any) -> None:
            follow_up_tasks: list[asyncio.Task[None]] = []

            async def on_outcome(outcome: Any) -> None:
                for emitted_event in outcome.emitted_events:
                    follow_up_tasks.append(asyncio.create_task(self._publish_impl(emitted_event)))

            dispatch_outcome = await self._runtime.dispatch_event(
                handlers,
                message,
                dispatch_id=dispatch_id,
                subscribers=self._subscribers,
                on_outcome=on_outcome,
            )
            failures.extend(dispatch_outcome.failures)
            if follow_up_tasks:
                nested_results = await asyncio.gather(*follow_up_tasks, return_exceptions=True)
                for result in nested_results:
                    if isinstance(result, EventPublicationError):
                        failures.extend(result.failures)
                    elif isinstance(result, Exception):
                        failures.append(result)
            if failures:
                raise EventPublicationError(failures)

        pipeline = compose_middleware(self._middleware, final_handler)
        try:
            await pipeline(event)
        except Exception:
            await self._runtime.notify_subscribers(
                self._subscribers,
                DispatchFinished(
                    message=event,
                    message_type=type(event),
                    operation="publish",
                    timestamp=datetime.now(),
                    dispatch_id=dispatch_id,
                    handler_count=len(handlers),
                    duration_ms=(perf_counter() - started) * 1000,
                    success=False,
                ),
            )
            raise

        await self._runtime.notify_subscribers(
            self._subscribers,
            DispatchFinished(
                message=event,
                message_type=type(event),
                operation="publish",
                timestamp=datetime.now(),
                dispatch_id=dispatch_id,
                handler_count=len(handlers),
                duration_ms=(perf_counter() - started) * 1000,
                success=True,
            ),
        )
        return [], []

    def send_sync(self, command: Any, timeout: float | None = None) -> Any:
        if self._in_running_loop_thread():
            raise BusUsageError(
                "send_sync() cannot run inside an active event loop; use await bus.send(...)"
            )
        return self._sync_bridge.run(self.send(command), timeout=timeout)

    def publish_sync(self, event: Any, timeout: float | None = None) -> None:
        if self._in_running_loop_thread():
            raise BusUsageError(
                "publish_sync() cannot run inside an active event loop; use await bus.publish(...)"
            )
        self._sync_bridge.run(self.publish(event), timeout=timeout)

    def close(self) -> None:
        if self._in_running_loop_thread():
            raise BusUsageError(
                "close() cannot run inside an active event loop; "
                "use await bus.aclose() from async code"
            )
        if self._sync_bridge._loop is None:
            asyncio.run(self._drain_and_close_runtime())
            return

        self._sync_bridge.run(self._drain_and_close_runtime())
        self._sync_bridge.close()

    async def aclose(self) -> None:
        await self._drain_and_close_runtime()
        await self._sync_bridge.aclose()

    async def _drain_and_close_runtime(self) -> None:
        await self._lifecycle.begin_close()
        await self._runtime.aclose()
        await self._lifecycle.finish_close()

    def _in_running_loop_thread(self) -> bool:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False
        return True
