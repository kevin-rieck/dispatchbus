import asyncio
import threading
from collections.abc import Coroutine, Sequence
from concurrent.futures import Executor, Future
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
        self._close_lock = threading.Lock()
        self._close_future: Future[None] | None = None
        self._sync_bridge_close_started = False

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
        with self._close_lock:
            self._dispatch_tree.check_admission()
            return self._sync_bridge.run(self.send(command), timeout=timeout)

    def publish_sync(self, event: Any, timeout: float | None = None) -> None:
        if self._in_running_loop_thread():
            raise BusUsageError(
                "publish_sync() cannot run inside an active event loop; use await bus.publish(...)"
            )
        with self._close_lock:
            self._dispatch_tree.check_admission()
            self._sync_bridge.run(self.publish(event), timeout=timeout)

    def close(self) -> None:
        if self._in_running_loop_thread():
            raise BusUsageError(
                "close() cannot run inside an active event loop; "
                "use await bus.aclose() from async code"
            )
        asyncio.run(self.aclose())

    async def aclose(self) -> None:
        await self._acquire_close_lock()
        try:
            if self._close_future is None:
                close_operation = self._dispatch_tree.aclose()
                self._close_future = Future()
                threading.Thread(
                    target=self._run_close_runtime,
                    args=(close_operation,),
                    daemon=True,
                ).start()
            close_future = self._close_future
        finally:
            self._close_lock.release()

        close_error: BaseException | None = None
        try:
            await asyncio.shield(asyncio.wrap_future(close_future))
        except BaseException as exc:
            close_error = exc
        if not isinstance(close_error, asyncio.CancelledError):
            bridge_error = await self._close_sync_bridge_if_needed()
            if bridge_error is not None and close_error is None:
                raise bridge_error
        if close_error is not None:
            raise close_error

    def _run_close_runtime(self, close_operation: Coroutine[Any, Any, None]) -> None:
        asyncio.run(self._close_runtime(close_operation))

    async def _close_runtime(self, close_operation: Coroutine[Any, Any, None]) -> None:
        assert self._close_future is not None
        close_future = self._close_future
        close_error: BaseException | None = None
        try:
            await close_operation
        except BaseException as exc:
            close_error = exc
        bridge_error = await self._close_sync_bridge_if_needed()
        if bridge_error is not None and close_error is None:
            close_error = bridge_error
        if close_error is not None:
            close_future.set_exception(close_error)
        else:
            close_future.set_result(None)

    async def _acquire_close_lock(self) -> None:
        while not self._close_lock.acquire(blocking=False):
            await asyncio.sleep(0)

    async def _close_sync_bridge_if_needed(self) -> BaseException | None:
        if self._sync_bridge.is_worker_thread():
            return None
        try:
            await asyncio.to_thread(self._close_sync_bridge_once)
        except BaseException as exc:
            return exc
        return None

    def _close_sync_bridge_once(self) -> None:
        with self._close_lock:
            if self._sync_bridge_close_started or not self._sync_bridge.is_started():
                return
            self._sync_bridge_close_started = True
        self._sync_bridge.close()

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
