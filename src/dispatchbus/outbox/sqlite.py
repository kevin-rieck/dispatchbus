import re
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import aiosqlite

from dispatchbus import MessageBase
from dispatchbus.messages import get_metadata
from dispatchbus.outbox.models import (
    MessageSerializer,
    OutboxMessage,
    OutboxRetentionPolicy,
    OutboxStorage,
)

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SQLiteOutboxStorage(OutboxStorage):
    def __init__(
        self,
        connection: aiosqlite.Connection,
        table_name: str = "dispatchbus_outbox",
        claim_timeout: timedelta = timedelta(minutes=5),
    ):
        if not _SQL_IDENTIFIER_RE.fullmatch(table_name):
            raise ValueError("table_name must be a valid SQL identifier")

        self.connection = connection
        self.table_name = table_name
        self.claim_timeout = claim_timeout

    async def get_pending_messages(self, batch_size: int) -> list[OutboxMessage]:
        return await self.claim_pending_messages(batch_size)

    async def claim_pending_messages(self, batch_size: int) -> list[OutboxMessage]:
        now = datetime.now(UTC)
        stale_before = (now - self.claim_timeout).isoformat()
        claimed_at = now.isoformat()

        await self.connection.execute("BEGIN")
        try:
            reclaim_query = f"""
                UPDATE {self.table_name}
                SET claimed_at = NULL
                WHERE published_at IS NULL
                  AND claimed_at IS NOT NULL
                  AND claimed_at <= ?
            """
            await self.connection.execute(reclaim_query, (stale_before,))

            select_ids_query = f"""
                SELECT id
                FROM {self.table_name}
                WHERE published_at IS NULL
                  AND claimed_at IS NULL
                ORDER BY created_at ASC
                LIMIT ?
            """
            async with self.connection.execute(select_ids_query, (batch_size,)) as cursor:
                rows = await cursor.fetchall()

            message_ids = [row[0] for row in rows]
            if not message_ids:
                await self.connection.commit()
                return []

            placeholders = ",".join("?" for _ in message_ids)
            claim_query = f"""
                UPDATE {self.table_name}
                SET claimed_at = ?
                WHERE published_at IS NULL
                  AND claimed_at IS NULL
                  AND id IN ({placeholders})
            """
            await self.connection.execute(claim_query, [claimed_at, *message_ids])

            fetch_query = f"""
                SELECT id, message_type, payload, created_at, claimed_at, published_at
                FROM {self.table_name}
                WHERE claimed_at = ?
                  AND id IN ({placeholders})
                ORDER BY created_at ASC
            """
            async with self.connection.execute(fetch_query, [claimed_at, *message_ids]) as cursor:
                claimed_rows = await cursor.fetchall()

            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise

        return [
            OutboxMessage(
                id=row[0],
                message_type=row[1],
                payload=row[2],
                created_at=datetime.fromisoformat(row[3]),
                claimed_at=datetime.fromisoformat(row[4]) if row[4] else None,
                published_at=datetime.fromisoformat(row[5]) if row[5] else None,
            )
            for row in claimed_rows
        ]

    async def mark_as_published(self, message_ids: list[str]) -> None:
        if not message_ids:
            return

        placeholders = ",".join("?" for _ in message_ids)
        query = f"""
            UPDATE {self.table_name}
            SET claimed_at = NULL,
                published_at = ?
            WHERE id IN ({placeholders})
        """
        params = [datetime.now(UTC).isoformat(), *message_ids]
        await self.connection.execute(query, params)
        await self.connection.commit()

    async def release_claims(self, message_ids: list[str]) -> None:
        if not message_ids:
            return

        placeholders = ",".join("?" for _ in message_ids)
        query = f"""
            UPDATE {self.table_name}
            SET claimed_at = NULL
            WHERE published_at IS NULL
              AND id IN ({placeholders})
        """
        await self.connection.execute(query, message_ids)
        await self.connection.commit()

    async def evict_published_messages(self, policy: OutboxRetentionPolicy) -> int:
        cutoff = (datetime.now(UTC) - policy.retention_period).isoformat()
        deleted = 0

        await self.connection.execute("BEGIN")
        try:
            delete_old_query = f"""
                DELETE FROM {self.table_name}
                WHERE published_at IS NOT NULL
                  AND published_at < ?
            """
            cursor = await self.connection.execute(delete_old_query, (cutoff,))
            deleted += cursor.rowcount

            if policy.max_published_rows is not None:
                trim_query = f"""
                    DELETE FROM {self.table_name}
                    WHERE id IN (
                        SELECT id
                        FROM {self.table_name}
                        WHERE published_at IS NOT NULL
                        ORDER BY published_at DESC
                        LIMIT -1 OFFSET ?
                    )
                """
                trim_cursor = await self.connection.execute(
                    trim_query, (policy.max_published_rows,)
                )
                deleted += trim_cursor.rowcount

            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise

        return deleted

    async def enqueue(
        self,
        message: MessageBase,
        serializer: MessageSerializer,
        *,
        message_id: str | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """
        Serializes and inserts a message into the outbox table.

        The insertion uses the configured `aiosqlite.Connection` and does NOT auto-commit.
        Callers must commit the connection to atomically save the outbox row alongside business
        data.
        """
        resolved_id = message_id
        resolved_created_at = created_at

        if not resolved_id or not resolved_created_at:
            try:
                meta = get_metadata(message)
                resolved_id = resolved_id or meta.message_id
                resolved_created_at = resolved_created_at or meta.timestamp
            except ValueError:
                pass

        resolved_id = resolved_id or uuid4().hex
        resolved_created_at = resolved_created_at or datetime.now(UTC)

        payload = serializer.serialize(message)
        message_type = getattr(message, "message_name", type(message).__name__)

        query = f"""
            INSERT INTO {self.table_name}
            (id, message_type, payload, created_at)
            VALUES (?, ?, ?, ?)
        """
        await self.connection.execute(
            query,
            (resolved_id, message_type, payload, resolved_created_at.isoformat()),
        )
