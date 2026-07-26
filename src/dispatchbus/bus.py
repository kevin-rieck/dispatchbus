import asyncio
from collections.abc import Sequence
from concurrent.futures import Executor
from typing import Any

from dispatchbus.dispatch_tree import DispatchTree, EventConcurrency
from dispatchbus.exceptions import BusUsageError
from dispatchbus.handler_runtime import ErrorHandler
from dispatchbus.messages import CommandBase, EventBase, as_runtime_message
from dispatchbus.middleware import Middleware
from dispatchbus.observability import Subscriber
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
    ) -> None:
        self._dispatch_tree = DispatchTree(
            middleware,
            event_concurrency=event_concurrency,
            subscribers=subscribers,
            executor=executor,
            error_handler=error_handler,
        )
        self._sync_bridge = SyncBridge()

    async def __aenter__(self) -> "MessageBus":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

    def __enter__(self) -> "MessageBus":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._dispatch_tree.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._dispatch_tree.register_event_handler(message_type, handler)

    def add_subscriber(self, subscriber: Subscriber) -> None:
        self._dispatch_tree.add_subscriber(subscriber)

    async def send(self, command: Any) -> Any:
        self._require_command_instance(command)
        return await self._dispatch_tree.send(as_runtime_message(command))

    async def publish(self, event: Any) -> None:
        self._require_event_instance(event)
        await self._dispatch_tree.publish(as_runtime_message(event))

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
        await self._dispatch_tree.aclose()

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
