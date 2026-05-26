import asyncio
import threading
from concurrent.futures import Future
from typing import Any


class SyncBridge:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._loop_ready = threading.Event()
        self._loop_start_lock = threading.Lock()
        self._loop_error: Exception | None = None

    def run(self, coroutine: Any, timeout: float | None = None) -> Any:
        self._ensure_background_loop()
        assert self._loop is not None
        future: Future[Any] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result(timeout=timeout)

    def close(self) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._loop.stop)
        assert self._thread is not None
        self._thread.join(timeout=1)
        self._loop = None
        self._thread = None
        self._loop_ready.clear()
        self._loop_error = None

    async def aclose(self) -> None:
        self.close()

    def _ensure_background_loop(self) -> None:
        if self._loop is not None:
            return
        with self._loop_start_lock:
            if self._loop is not None:
                return
            self._loop_ready.clear()
            self._loop_error = None
            self._thread = threading.Thread(target=self._run_background_loop, daemon=True)
            self._thread.start()
            self._loop_ready.wait()
            if self._loop_error is not None:
                raise RuntimeError("Failed to start background event loop") from self._loop_error
            if self._loop is None:
                raise RuntimeError("Background event loop failed to start")

    def _run_background_loop(self) -> None:
        try:
            loop = asyncio.new_event_loop()
            self._loop = loop
            asyncio.set_event_loop(loop)
        except Exception as exc:
            self._loop_error = exc
            self._loop_ready.set()
            return

        self._loop_ready.set()
        try:
            loop.run_forever()
        finally:
            loop.close()
