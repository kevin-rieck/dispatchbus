import asyncio
from collections.abc import Sequence
from concurrent.futures import Executor
from typing import Any

from dispatchbus.command_dispatch import CommandDispatcher
from dispatchbus.event_publisher import EventPublisher
from dispatchbus.exceptions import BusUsageError
from dispatchbus.lifecycle import BusLifecycle
from dispatchbus.messages import CommandBase, EventBase, as_runtime_message
from dispatchbus.middleware import Middleware
from dispatchbus.observability import Subscriber
from dispatchbus.registry import HandlerRegistry
from dispatchbus.runtime import ErrorHandler, EventConcurrency, MessageRuntime
from dispatchbus.sync_bridge import SyncBridge


class MessageBus:
    def __init__(
        self,
        middleware: Sequence[Middleware] | None = None,
        *,
        event_concurrency: EventConcurrency = "concurrent",
        subscribers: Sequence[Subscriber] | None = None,
        executor: Executor | None = None,
        error_handler: ErrorHandler | None = None,
        max_dispatch_chain_length: int | None = None,
    ) -> None:
        if max_dispatch_chain_length is not None and max_dispatch_chain_length <= 0:
            raise ValueError("max_dispatch_chain_length must be a positive integer or None")
        self._registry = HandlerRegistry()
        self._runtime = MessageRuntime(
            event_concurrency=event_concurrency,
            executor=executor,
            error_handler=error_handler,
        )
        self._middleware = list(middleware or [])
        self._subscribers = list(subscribers or [])
        self._lifecycle = BusLifecycle()
        self._max_dispatch_chain_length = max_dispatch_chain_length
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

    async def __aenter__(self) -> "MessageBus":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

    def __enter__(self) -> "MessageBus":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_event_handler(message_type, handler)

    def add_subscriber(self, subscriber: Subscriber) -> None:
        self._subscribers.append(subscriber)

    async def send(self, command: Any) -> Any:
        self._require_command_instance(command)
        token = await self._lifecycle.enter_send()
        try:
            return await self._send_impl(as_runtime_message(command))
        finally:
            await self._lifecycle.leave_dispatch(token)

    async def _send_impl(self, command: Any) -> Any:
        return await self._command_dispatcher.send(command)

    async def publish(self, event: Any) -> None:
        self._require_event_instance(event)
        token = await self._lifecycle.enter_publish()
        try:
            await self._publish_impl(as_runtime_message(event))
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

    def _require_command_instance(self, message: Any) -> None:
        if not isinstance(message, CommandBase):
            raise BusUsageError(
                f"send() requires a CommandBase instance, got {type(message).__name__}"
            )

    def _require_event_instance(self, message: Any) -> None:
        if not isinstance(message, EventBase):
            raise BusUsageError(
                f"publish() requires an EventBase instance, got {type(message).__name__}"
            )

    def _in_running_loop_thread(self) -> bool:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False
        return True
