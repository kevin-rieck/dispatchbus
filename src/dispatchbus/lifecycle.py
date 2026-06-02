import asyncio
import contextvars
import threading
from enum import Enum, auto

from dispatchbus.exceptions import BusDrainingError, MaxDispatchChainLengthExceededError


class BusState(Enum):
    OPEN = auto()
    DRAINING = auto()
    CLOSED = auto()


class BusLifecycle:
    def __init__(self, *, max_dispatch_chain_length: int | None = None) -> None:
        if max_dispatch_chain_length is not None and max_dispatch_chain_length <= 0:
            raise ValueError("max_dispatch_chain_length must be a positive integer or None")
        self._max_dispatch_chain_length = max_dispatch_chain_length
        self._state = BusState.OPEN
        self._in_flight_dispatches = 0
        self._state_lock = threading.Lock()
        self._drained = threading.Event()
        self._drained.set()
        self._active_dispatch_depth: contextvars.ContextVar[int] = contextvars.ContextVar(
            "dispatchbus_bus_active_dispatch_depth",
            default=0,
        )

    async def enter_send(self) -> contextvars.Token[int]:
        return self._enter("send")

    async def enter_publish(self) -> contextvars.Token[int]:
        return self._enter("publish")

    def _enter(self, operation: str) -> contextvars.Token[int]:
        current_depth = self._active_dispatch_depth.get()
        attempted_depth = current_depth + 1
        with self._state_lock:
            if self._state is BusState.CLOSED:
                raise BusDrainingError("message bus is draining")
            if operation == "send" and self._state is not BusState.OPEN:
                raise BusDrainingError("message bus is draining")
            if operation == "publish" and self._state is BusState.DRAINING and current_depth == 0:
                raise BusDrainingError("message bus is draining")
            if (
                self._max_dispatch_chain_length is not None
                and attempted_depth > self._max_dispatch_chain_length
            ):
                raise MaxDispatchChainLengthExceededError(
                    self._max_dispatch_chain_length,
                    attempted_depth,
                )
            self._in_flight_dispatches += 1
            self._drained.clear()
        return self._active_dispatch_depth.set(attempted_depth)

    async def leave_dispatch(self, token: contextvars.Token[int]) -> None:
        self._active_dispatch_depth.reset(token)
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
