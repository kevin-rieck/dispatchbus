from datetime import UTC, datetime

import aiosqlite
import pytest
import pytest_asyncio

from dispatchr.outbox.sqlite import SQLiteOutboxStorage


@pytest_asyncio.fixture
async def memory_db():
    async with aiosqlite.connect(":memory:") as db:
        await db.execute("""
            CREATE TABLE dispatchr_outbox (
                id TEXT PRIMARY KEY,
                message_type TEXT NOT NULL,
                payload BLOB NOT NULL,
                created_at TIMESTAMP NOT NULL,
                published_at TIMESTAMP
            )
        """)
        yield db


@pytest.mark.asyncio
async def test_sqlite_storage(memory_db):
    storage = SQLiteOutboxStorage(memory_db)

    # 1. Insert a mock message directly
    now = datetime.now(UTC)
    await memory_db.execute(
        "INSERT INTO dispatchr_outbox (id, message_type, payload, created_at) VALUES (?, ?, ?, ?)",
        ("msg-1", "user.created", b"{}", now.isoformat()),
    )
    await memory_db.commit()

    # 2. Test get_pending_messages
    pending = await storage.get_pending_messages(batch_size=10)
    assert len(pending) == 1
    assert pending[0].id == "msg-1"
    assert pending[0].message_type == "user.created"
    assert pending[0].payload == b"{}"

    # 3. Test mark_as_published
    await storage.mark_as_published(["msg-1"])

    # 4. Verify it's no longer pending
    pending_after = await storage.get_pending_messages(batch_size=10)
    assert len(pending_after) == 0

    # 5. Verify published_at is timezone-aware
    async with memory_db.execute(
        "SELECT published_at FROM dispatchr_outbox WHERE id = 'msg-1'"
    ) as cursor:
        row = await cursor.fetchone()
        assert row is not None
        published_at_str = row[0]
        published_at = datetime.fromisoformat(published_at_str)
        assert published_at.tzinfo is not None
