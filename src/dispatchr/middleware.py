from collections.abc import Awaitable, Callable, Sequence
from typing import Any

NextCallable = Callable[[Any], Awaitable[Any]]
Middleware = Callable[[Any, NextCallable], Awaitable[Any]]


def compose_middleware(
    middleware: Sequence[Middleware],
    final_handler: NextCallable,
) -> NextCallable:
    call_next = final_handler
    for current in reversed(middleware):
        previous = call_next

        async def wrapper(message: Any, current=current, previous=previous) -> Any:
            return await current(message, previous)

        call_next = wrapper
    return call_next
