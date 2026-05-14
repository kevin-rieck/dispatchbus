from typing import Protocol


class Command(Protocol):
    """Marker protocol for command messages."""


class Event(Protocol):
    """Marker protocol for event messages."""
