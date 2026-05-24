import asyncio
import contextvars
import threading
from collections import deque
from collections.abc import Sequence
from concurrent.futures import Future
from datetime import datetime
from enum import Enum, auto
from time import perf_counter
from typing import Any

from dispatchr.exceptions import BusDrainingError, BusUsageError, EventPublicationError
from dispatchr.middleware import Middleware, compose_middleware
from dispatchr.observability import DispatchFinished, DispatchStarted, Subscriber, new_dispatch_id
from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import EventConcurrency, MessageRuntime


class _BusState(Enum):
    OPEN = auto()
    DRAINING = auto()
    CLOSED = auto()


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
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._loop_ready = threading.Event()
        self._loop_start_lock = threading.Lock()
        self._state = _BusState.OPEN
        self._in_flight_dispatches = 0
        self._state_lock = threading.Lock()
        self._drained = threading.Event()
        self._drained.set()
        self._accepted_publish_depth: contextvars.ContextVar[int] = contextvars.ContextVar(
            "dispatchr_bus_accepted_publish_depth",
            default=0,
        )

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_event_handler(message_type, handler)

    def add_subscriber(self, subscriber: Subscriber) -> None:
        self._subscribers.append(subscriber)

    async def send(self, command: Any) -> Any:
        token, counted = await self._enter_dispatch(operation="send")
        try:
            return await self._send_impl(command)
        finally:
            await self._leave_dispatch(token, counted)

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
        token, counted = await self._enter_dispatch(operation="publish")
        try:
            await self._publish_impl(event)
        finally:
            await self._leave_dispatch(token, counted)

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
        return self._run_sync(self.send(command), timeout=timeout)

    def publish_sync(self, event: Any, timeout: float | None = None) -> None:
        if self._in_running_loop_thread():
            raise BusUsageError(
                "publish_sync() cannot run inside an active event loop; use await bus.publish(...)"
            )
        self._run_sync(self.publish(event), timeout=timeout)

    def close(self) -> None:
        if self._in_running_loop_thread():
            raise BusUsageError(
                "close() cannot run inside an active event loop; use await bus.aclose() from async code"
            )
        if self._loop is None:
            asyncio.run(self._drain_and_close_runtime())
            return

        future = asyncio.run_coroutine_threadsafe(self._drain_and_close_runtime(), self._loop)
        future.result()
        self._loop.call_soon_threadsafe(self._loop.stop)
        assert self._thread is not None
        self._thread.join(timeout=1)
        self._loop = None
        self._thread = None
        self._loop_ready.clear()

    async def aclose(self) -> None:
        await self._drain_and_close_runtime()
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
            assert self._thread is not None
            self._thread.join(timeout=1)
            self._loop = None
            self._thread = None
            self._loop_ready.clear()

    async def _drain_and_close_runtime(self) -> None:
        with self._state_lock:
            if self._state is _BusState.CLOSED:
                return
            self._state = _BusState.DRAINING
            drained = self._drained.is_set()

        if not drained:
            await asyncio.to_thread(self._drained.wait)

        with self._state_lock:
            self._state = _BusState.CLOSED

        await self._runtime.aclose()

    async def _enter_dispatch(self, *, operation: str) -> tuple[contextvars.Token[int], bool]:
        current_depth = self._accepted_publish_depth.get()
        with self._state_lock:
            if self._state is _BusState.CLOSED:
                raise BusDrainingError("message bus is draining")
            if operation == "send" and self._state is not _BusState.OPEN:
                raise BusDrainingError("message bus is draining")
            if operation == "publish" and self._state is _BusState.DRAINING and current_depth == 0:
                raise BusDrainingError("message bus is draining")
            self._in_flight_dispatches += 1
            self._drained.clear()
        return self._accepted_publish_depth.set(current_depth + 1), True

    async def _leave_dispatch(self, token: contextvars.Token[int], counted: bool) -> None:
        self._accepted_publish_depth.reset(token)
        if not counted:
            return
        with self._state_lock:
            self._in_flight_dispatches -= 1
            if self._in_flight_dispatches == 0:
                self._drained.set()

    def _run_sync(self, coroutine: Any, timeout: float | None = None) -> Any:
        self._ensure_background_loop()
        assert self._loop is not None
        future: Future[Any] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result(timeout=timeout)

    def _in_running_loop_thread(self) -> bool:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False
        return True

    def _ensure_background_loop(self) -> None:
        if self._loop is not None:
            return
        with self._loop_start_lock:
            if self._loop is not None:
                return
            self._loop_ready.clear()
            self._thread = threading.Thread(target=self._run_background_loop, daemon=True)
            self._thread.start()
            self._loop_ready.wait()

    def _run_background_loop(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        self._loop_ready.set()
        loop.run_forever()
        loop.close()
