import asyncio
import threading
from collections.abc import Sequence
from concurrent.futures import Future
from typing import Any

from dispatchr.middleware import Middleware, compose_middleware
from dispatchr.registry import HandlerRegistry
from dispatchr.runtime import MessageRuntime


class MessageBus:
    def __init__(
        self,
        middleware: Sequence[Middleware] | None = None,
        *,
        event_concurrency: str = "concurrent",
    ) -> None:
        self._registry = HandlerRegistry()
        self._runtime = MessageRuntime(event_concurrency=event_concurrency)
        self._middleware = list(middleware or [])
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._loop_ready = threading.Event()

    def register_command_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_command_handler(message_type, handler)

    def register_event_handler(self, message_type: type[Any], handler: Any) -> None:
        self._registry.register_event_handler(message_type, handler)

    async def send(self, command: Any) -> Any:
        handler = self._registry.get_command_handler(type(command))

        async def final_handler(message: Any) -> Any:
            return await self._runtime.dispatch_command(handler, message)

        pipeline = compose_middleware(self._middleware, final_handler)
        return await pipeline(command)

    async def publish(self, event: Any) -> None:
        handlers = self._registry.get_event_handlers(type(event))

        async def final_handler(message: Any) -> None:
            await self._runtime.dispatch_event(handlers, message)

        pipeline = compose_middleware(self._middleware, final_handler)
        await pipeline(event)

    def send_sync(self, command: Any, timeout: float | None = None) -> Any:
        return self._run_sync(self.send(command), timeout=timeout)

    def publish_sync(self, event: Any, timeout: float | None = None) -> None:
        self._run_sync(self.publish(event), timeout=timeout)

    def close(self) -> None:
        if self._loop is None:
            return
        future = asyncio.run_coroutine_threadsafe(self._runtime.aclose(), self._loop)
        future.result()
        self._loop.call_soon_threadsafe(self._loop.stop)
        assert self._thread is not None
        self._thread.join(timeout=1)
        self._loop = None
        self._thread = None
        self._loop_ready.clear()

    async def aclose(self) -> None:
        await self._runtime.aclose()

    def _run_sync(self, coroutine: Any, timeout: float | None = None) -> Any:
        self._ensure_background_loop()
        assert self._loop is not None
        future: Future[Any] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result(timeout=timeout)

    def _ensure_background_loop(self) -> None:
        if self._loop is not None:
            return
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
