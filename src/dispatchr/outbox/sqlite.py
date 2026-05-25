from datetime import datetime

import aiosqlite

from dispatchr.outbox import OutboxMessage, OutboxStorage


class SQLiteOutboxStorage(OutboxStorage):
    def __init__(self, connection: aiosqlite.Connection, table_name: str = "dispatchr_outbox"):
        self.connection = connection
        self.table_name = table_name

    async def get_pending_messages(self, batch_size: int) -> list[OutboxMessage]:
        query = f"""
            SELECT id, message_type, payload, created_at, published_at
            FROM {self.table_name}
            WHERE published_at IS NULL
            ORDER BY created_at ASC
            LIMIT ?
        """
        async with self.connection.execute(query, (batch_size,)) as cursor:
            rows = await cursor.fetchall()

        messages = []
        for row in rows:
            messages.append(
                OutboxMessage(
                    id=row[0],
                    message_type=row[1],
                    payload=row[2],
                    created_at=datetime.fromisoformat(row[3]),
                    published_at=datetime.fromisoformat(row[4]) if row[4] else None,
                )
            )
        return messages

    async def mark_as_published(self, message_ids: list[str]) -> None:
        if not message_ids:
            return

        placeholders = ",".join("?" for _ in message_ids)
        query = f"""
            UPDATE {self.table_name}
            SET published_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
        """
        await self.connection.execute(query, message_ids)
        await self.connection.commit()
