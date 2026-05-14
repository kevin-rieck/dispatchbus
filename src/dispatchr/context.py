from collections.abc import Sequence


class EventContext:
    def __init__(self) -> None:
        self._events: list[object] = []

    def emit(self, event: object) -> None:
        self._events.append(event)

    @property
    def events(self) -> Sequence[object]:
        return tuple(self._events)
