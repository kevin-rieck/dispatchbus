import asyncio
from collections.abc import Sequence
from datetime import datetime
from time import perf_counter
from typing import Any

from dispatchr.command_dispatch import CommandDispatcher
from dispatchr.event_publisher import EventPublisher
from dispatchr.exceptions import BusUsageError
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
        self._event_publisher = EventPublisher(
            registry=self._registry,
            runtime=self._runtime,
            middleware=self._middleware,
            subscribers=self._subscribers,
        )
        self._command_dispatcher = CommandDispatcher(
            registry=self._registry,
            runtime=self._runtime,
            middleware=self._middleware,
            subscribers=self._subscribers,
            publish_event=self._event_publisher.publish,
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
            return await self._send_impl(command)
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _send_impl(self, command: Any) -> Any:
        return await self._command_dispatcher.send(command)

    async def publish(self, event: Any) -> None:
        token = await self._lifecycle.enter_publish()
        try:
            await self._publish_impl(event)
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _publish_impl(self, event: Any) -> None:
        await self._event_publisher.publish(event)

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
