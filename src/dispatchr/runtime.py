import asyncio
import inspect
from collections.abc import Callable
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Any, Literal

from dispatchr.exceptions import EventPublicationError, HandlerRegistrationError

Handler = Callable[[Any], Any]
EventConcurrency = Literal["concurrent", "sequential"]


class MessageRuntime:
    def __init__(
        self,
        executor: Executor | None = None,
        *,
        event_concurrency: EventConcurrency = "concurrent",
    ) -> None:
        if event_concurrency not in {"concurrent", "sequential"}:
            raise HandlerRegistrationError("event_concurrency must be 'concurrent' or 'sequential'")
        self._executor = executor or ThreadPoolExecutor()
        self._owns_executor = executor is None
        self._event_concurrency = event_concurrency
        self._in_flight: set[asyncio.Task[Any]] = set()

    async def dispatch_command(self, handler: Handler, message: Any) -> Any:
        return await self._call_handler(handler, message)

    async def dispatch_event(self, handlers: list[Handler], message: Any) -> None:
        if self._event_concurrency == "sequential":
            failures: list[Exception] = []
            for handler in handlers:
                try:
                    await self._call_handler(handler, message)
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise EventPublicationError(failures)
            return

        tasks = [asyncio.create_task(self._call_handler(handler, message)) for handler in handlers]
        for task in tasks:
            self._in_flight.add(task)
            task.add_done_callback(self._in_flight.discard)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            raise EventPublicationError(failures)

    async def _call_handler(self, handler: Handler, message: Any) -> Any:
        if inspect.iscoroutinefunction(handler):
            return await handler(message)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, handler, message)

    async def aclose(self) -> None:
        if self._in_flight:
            await asyncio.gather(*list(self._in_flight), return_exceptions=True)
        if self._owns_executor:
            self._executor.shutdown(wait=True)
