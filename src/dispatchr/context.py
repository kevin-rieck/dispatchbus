from collections.abc import Sequence

from dispatchr.exceptions import InvalidMessageError
from dispatchr.messages import EventBase, MessageMetadata, derive_child_metadata


class EventContext:
    def __init__(self, parent_metadata: MessageMetadata) -> None:
        self._events: list[EventBase] = []
        self._parent_metadata = parent_metadata

    def emit(self, event: object) -> None:
        if not isinstance(event, EventBase):
            raise InvalidMessageError(
                f"emit() requires an EventBase instance, got {type(event).__name__}"
            )
        stamped_event = event.with_metadata(derive_child_metadata(self._parent_metadata))
        self._events.append(stamped_event)

    @property
    def events(self) -> Sequence[EventBase]:
        return tuple(self._events)
