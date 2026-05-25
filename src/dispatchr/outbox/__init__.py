from typing import TYPE_CHECKING, Any

from .models import (
    EventPublisher,
    MessageSerializer,
    OutboxMessage,
    OutboxRetentionPolicy,
    OutboxStorage,
)
from .processor import OutboxProcessor, OutboxWorker
from .serializers import JSONSerializer

if TYPE_CHECKING:
    from .sqlite import SQLiteOutboxStorage

__all__ = [
    "OutboxMessage",
    "OutboxRetentionPolicy",
    "OutboxStorage",
    "MessageSerializer",
    "EventPublisher",
    "JSONSerializer",
    "OutboxProcessor",
    "OutboxWorker",
    "SQLiteOutboxStorage",
]


def __getattr__(name: str) -> Any:
    if name != "SQLiteOutboxStorage":
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)

    try:
        from .sqlite import SQLiteOutboxStorage
    except ModuleNotFoundError as exc:
        if exc.name == "aiosqlite":
            raise ImportError(
                "SQLiteOutboxStorage requires the optional 'sqlite' dependency. "
                "Install it with: pip install dispatchr[sqlite]"
            ) from exc
        raise

    return SQLiteOutboxStorage
