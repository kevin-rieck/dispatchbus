import inspect
from collections.abc import Callable
from typing import Any

from dispatchbus.exceptions import BusUsageError


def is_async_callable(value: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(value) or (
        callable(value) and inspect.iscoroutinefunction(value.__call__)
    )


def ensure_sync_result(result: Any, *, kind: str) -> Any:
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise BusUsageError(f"sync {kind} returned an awaitable; declare it with async def")
    return result
