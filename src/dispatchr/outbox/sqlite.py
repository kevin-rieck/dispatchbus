import re
from datetime import UTC, datetime, timedelta

import aiosqlite

from dispatchr.outbox.models import OutboxMessage, OutboxStorage

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SQLiteOutboxStorage(OutboxStorage):
    def __init__(
        self,
        connection: aiosqlite.Connection,
        table_name: str = "dispatchr_outbox",
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
