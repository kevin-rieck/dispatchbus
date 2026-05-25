from collections.abc import Sequence

from dispatchr.exceptions import InvalidMessageError
from dispatchr.messages import EventBase, MessageMetadata, RuntimeMessage, as_runtime_message


class EventContext:
    def __init__(self, parent_metadata: MessageMetadata) -> None:
        self._events: list[RuntimeMessage] = []
        self._parent_metadata = parent_metadata

    def emit(self, event: object) -> None:
        runtime_event = as_runtime_message(event, parent=self._parent_metadata)
        if not isinstance(runtime_event.payload, EventBase):
            raise InvalidMessageError(
                f"emit() requires an EventBase instance, got {type(event).__name__}"
            )
        self._events.append(runtime_event)

    @property
    def events(self) -> Sequence[RuntimeMessage]:
        return tuple(self._events)
