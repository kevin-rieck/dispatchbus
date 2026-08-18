import asyncio
import contextvars
import threading
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from enum import Enum, auto
from typing import Literal

from dispatchbus.exceptions import BusDrainingError


class BusState(Enum):
    OPEN = auto()
    DRAINING = auto()
    CLOSED = auto()


class BusLifecycle:
    def __init__(self) -> None:
        self._state = BusState.OPEN
        self._in_flight_dispatches = 0
        self._state_lock = threading.Lock()
        self._drained = threading.Event()
        self._drained.set()
        self._accepted_dispatch_depth: contextvars.ContextVar[int] = contextvars.ContextVar(
            "dispatchbus_bus_accepted_dispatch_depth",
            default=0,
        )

    def admit_command(self) -> AbstractAsyncContextManager[None]:
        return self._admit("send")

    def admit_event(self) -> AbstractAsyncContextManager[None]:
        return self._admit("publish")

    @asynccontextmanager
    async def _admit(self, operation: Literal["send", "publish"]) -> AsyncIterator[None]:
        token = self._enter(operation)
        try:
            yield
        finally:
            self._leave_dispatch(token)

    async def enter_publish(self) -> contextvars.Token[int]:
        return self._enter("publish")

    def _enter(self, operation: str) -> contextvars.Token[int]:
        current_depth = self._accepted_dispatch_depth.get()
        with self._state_lock:
            if self._state is BusState.CLOSED:
                raise BusDrainingError("message bus is draining")
            if operation == "send" and self._state is not BusState.OPEN:
                raise BusDrainingError("message bus is draining")
            if operation == "publish" and self._state is BusState.DRAINING and current_depth == 0:
                raise BusDrainingError("message bus is draining")
            self._in_flight_dispatches += 1
            self._drained.clear()
        return self._accepted_dispatch_depth.set(current_depth + 1)

    async def leave_dispatch(self, token: contextvars.Token[int]) -> None:
        self._leave_dispatch(token)

    def _leave_dispatch(self, token: contextvars.Token[int]) -> None:
        self._accepted_dispatch_depth.reset(token)
        with self._state_lock:
            self._in_flight_dispatches -= 1
            if self._in_flight_dispatches == 0:
                self._drained.set()

    async def begin_close(self, *, block: bool = True) -> None:
        with self._state_lock:
            if self._state is BusState.CLOSED:
                return
            self._state = BusState.DRAINING
            drained = self._drained.is_set()
        if block and not drained:
            await asyncio.to_thread(self._drained.wait)

    async def finish_close(self) -> None:
        with self._state_lock:
            self._state = BusState.CLOSED
