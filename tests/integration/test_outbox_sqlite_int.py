import importlib
import sys
from datetime import UTC, datetime, timedelta

import aiosqlite
import pytest
import pytest_asyncio


def test_outbox_package_imports_without_sqlite(monkeypatch):
    monkeypatch.setitem(sys.modules, "aiosqlite", None)
    sys.modules.pop("dispatchr.outbox", None)
    sys.modules.pop("dispatchr.outbox.sqlite", None)

    module = importlib.import_module("dispatchr.outbox")

    assert module.OutboxStorage is not None
    assert module.OutboxMessage is not None


def test_sqlite_outbox_storage_gives_actionable_error_without_sqlite(monkeypatch):
    monkeypatch.setitem(sys.modules, "aiosqlite", None)
    sys.modules.pop("dispatchr.outbox", None)
    sys.modules.pop("dispatchr.outbox.sqlite", None)

    module = importlib.import_module("dispatchr.outbox")

    with pytest.raises(ImportError, match=r"pip install dispatchr\[sqlite\]"):
        _ = module.SQLiteOutboxStorage


@pytest_asyncio.fixture
async def memory_db():
    async with aiosqlite.connect(":memory:") as db:
        await db.execute("""
            CREATE TABLE dispatchr_outbox (
                id TEXT PRIMARY KEY,
                message_type TEXT NOT NULL,
                payload BLOB NOT NULL,
                created_at TIMESTAMP NOT NULL,
                claimed_at TIMESTAMP,
                published_at TIMESTAMP
            )
        """)
        yield db


def test_sqlite_storage_rejects_invalid_table_name(memory_db):
    from dispatchr.outbox.sqlite import SQLiteOutboxStorage

    with pytest.raises(ValueError, match="table_name"):
        SQLiteOutboxStorage(memory_db, table_name="dispatchr_outbox; DROP TABLE x")


@pytest.mark.asyncio
async def test_sqlite_storage_claims_pending_messages(memory_db):
    from dispatchr.outbox.sqlite import SQLiteOutboxStorage

    storage = SQLiteOutboxStorage(memory_db)
    now = datetime.now(UTC)

    await memory_db.execute(
        "INSERT INTO dispatchr_outbox (id, message_type, payload, created_at) VALUES (?, ?, ?, ?)",
        ("msg-1", "user.created", b"{}", now.isoformat()),
    )
    await memory_db.commit()

    pending = await storage.claim_pending_messages(batch_size=10)

    assert [msg.id for msg in pending] == ["msg-1"]
    async with memory_db.execute(
        "SELECT claimed_at, published_at FROM dispatchr_outbox WHERE id = 'msg-1'"
    ) as cursor:
        row = await cursor.fetchone()

    assert row is not None
    assert row[0] is not None
    assert row[1] is None


@pytest.mark.asyncio
async def test_sqlite_storage_does_not_double_claim_across_instances(memory_db):
    from dispatchr.outbox.sqlite import SQLiteOutboxStorage

    storage1 = SQLiteOutboxStorage(memory_db)
    storage2 = SQLiteOutboxStorage(memory_db)
    now = datetime.now(UTC)

    await memory_db.execute(
        "INSERT INTO dispatchr_outbox (id, message_type, payload, created_at) VALUES (?, ?, ?, ?)",
        ("msg-1", "user.created", b"{}", now.isoformat()),
    )
    await memory_db.commit()

    first = await storage1.claim_pending_messages(batch_size=10)
    second = await storage2.claim_pending_messages(batch_size=10)

    assert [msg.id for msg in first] == ["msg-1"]
    assert second == []


@pytest.mark.asyncio
async def test_sqlite_storage_reclaims_stale_claims(memory_db):
    from dispatchr.outbox.sqlite import SQLiteOutboxStorage

    storage = SQLiteOutboxStorage(memory_db, claim_timeout=timedelta(seconds=0))
    now = datetime.now(UTC)

    await memory_db.execute(
        (
            "INSERT INTO dispatchr_outbox "
            "(id, message_type, payload, created_at, claimed_at) VALUES (?, ?, ?, ?, ?)"
        ),
        ("msg-1", "user.created", b"{}", now.isoformat(), now.isoformat()),
    )
    await memory_db.commit()

    pending = await storage.claim_pending_messages(batch_size=10)

    assert [msg.id for msg in pending] == ["msg-1"]


@pytest.mark.asyncio
async def test_sqlite_storage_release_claims_makes_message_pending_again(memory_db):
    from dispatchr.outbox.sqlite import SQLiteOutboxStorage

    storage = SQLiteOutboxStorage(memory_db)
    now = datetime.now(UTC)

    await memory_db.execute(
        "INSERT INTO dispatchr_outbox (id, message_type, payload, created_at) VALUES (?, ?, ?, ?)",
        ("msg-1", "user.created", b"{}", now.isoformat()),
    )
    await memory_db.commit()

    first = await storage.claim_pending_messages(batch_size=10)
    assert [msg.id for msg in first] == ["msg-1"]

    await storage.release_claims(["msg-1"])

    second = await storage.claim_pending_messages(batch_size=10)
    assert [msg.id for msg in second] == ["msg-1"]


@pytest.mark.asyncio
async def test_sqlite_storage(memory_db):
    from dispatchr.outbox.sqlite import SQLiteOutboxStorage

    storage = SQLiteOutboxStorage(memory_db)

    # 1. Insert a mock message directly
    now = datetime.now(UTC)
    await memory_db.execute(
        "INSERT INTO dispatchr_outbox (id, message_type, payload, created_at) VALUES (?, ?, ?, ?)",
        ("msg-1", "user.created", b"{}", now.isoformat()),
    )
    await memory_db.commit()

    # 2. Test claim_pending_messages
    pending = await storage.claim_pending_messages(batch_size=10)
    assert len(pending) == 1
    assert pending[0].id == "msg-1"
    assert pending[0].message_type == "user.created"
    assert pending[0].payload == b"{}"

    # 3. Test mark_as_published
    await storage.mark_as_published(["msg-1"])

    # 4. Verify it's no longer claimable
    pending_after = await storage.claim_pending_messages(batch_size=10)
    assert len(pending_after) == 0

    # 5. Verify claimed_at is cleared and published_at is timezone-aware
    async with memory_db.execute(
        "SELECT claimed_at, published_at FROM dispatchr_outbox WHERE id = 'msg-1'"
    ) as cursor:
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] is None
        published_at_str = row[1]
        published_at = datetime.fromisoformat(published_at_str)
        assert published_at.tzinfo is not None


@pytest.mark.asyncio
async def test_sqlite_storage_evicts_old_published_rows(memory_db):
    from dispatchr.outbox import OutboxRetentionPolicy
    from dispatchr.outbox.sqlite import SQLiteOutboxStorage

    storage = SQLiteOutboxStorage(memory_db)
    now = datetime.now(UTC)
    old_published_at = (now - timedelta(days=31)).isoformat()
    fresh_published_at = (now - timedelta(days=5)).isoformat()

    await memory_db.executemany(
        """
        INSERT INTO dispatchr_outbox
        (id, message_type, payload, created_at, published_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("old", "user.created", b"{}", now.isoformat(), old_published_at),
            ("fresh", "user.created", b"{}", now.isoformat(), fresh_published_at),
            ("pending", "user.created", b"{}", now.isoformat(), None),
        ],
    )
    await memory_db.commit()

    deleted = await storage.evict_published_messages(
        OutboxRetentionPolicy(retention_period=timedelta(days=30))
    )

    assert deleted == 1
    async with memory_db.execute("SELECT id FROM dispatchr_outbox ORDER BY id ASC") as cursor:
        rows = await cursor.fetchall()

    assert [row[0] for row in rows] == ["fresh", "pending"]


@pytest.mark.asyncio
async def test_sqlite_storage_trims_oldest_published_rows_to_max_count(memory_db):
    from dispatchr.outbox import OutboxRetentionPolicy
    from dispatchr.outbox.sqlite import SQLiteOutboxStorage

    storage = SQLiteOutboxStorage(memory_db)
    now = datetime.now(UTC)

    await memory_db.executemany(
        """
        INSERT INTO dispatchr_outbox
        (id, message_type, payload, created_at, published_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                "pub-1",
                "user.created",
                b"{}",
                now.isoformat(),
                (now - timedelta(days=3)).isoformat(),
            ),
            (
                "pub-2",
                "user.created",
                b"{}",
                now.isoformat(),
                (now - timedelta(days=2)).isoformat(),
            ),
            (
                "pub-3",
                "user.created",
                b"{}",
                now.isoformat(),
                (now - timedelta(days=1)).isoformat(),
            ),
        ],
    )
    await memory_db.commit()

    deleted = await storage.evict_published_messages(
        OutboxRetentionPolicy(retention_period=timedelta(days=30), max_published_rows=2)
    )

    assert deleted == 1
    async with memory_db.execute(
        "SELECT id FROM dispatchr_outbox ORDER BY published_at ASC"
    ) as cursor:
        rows = await cursor.fetchall()

    assert [row[0] for row in rows] == ["pub-2", "pub-3"]


@pytest.mark.asyncio
async def test_sqlite_storage_applies_age_then_count_trimming(memory_db):
    from dispatchr.outbox import OutboxRetentionPolicy
    from dispatchr.outbox.sqlite import SQLiteOutboxStorage

    storage = SQLiteOutboxStorage(memory_db)
    now = datetime.now(UTC)

    await memory_db.executemany(
        """
        INSERT INTO dispatchr_outbox
        (id, message_type, payload, created_at, published_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("old", "user.created", b"{}", now.isoformat(), (now - timedelta(days=45)).isoformat()),
            (
                "keep-1",
                "user.created",
                b"{}",
                now.isoformat(),
                (now - timedelta(days=4)).isoformat(),
            ),
            (
                "keep-2",
                "user.created",
                b"{}",
                now.isoformat(),
                (now - timedelta(days=3)).isoformat(),
            ),
            (
                "keep-3",
                "user.created",
                b"{}",
                now.isoformat(),
                (now - timedelta(days=2)).isoformat(),
            ),
        ],
    )
    await memory_db.commit()

    deleted = await storage.evict_published_messages(
        OutboxRetentionPolicy(retention_period=timedelta(days=30), max_published_rows=2)
    )

    assert deleted == 2
    async with memory_db.execute(
        "SELECT id FROM dispatchr_outbox ORDER BY published_at ASC"
    ) as cursor:
        rows = await cursor.fetchall()

    assert [row[0] for row in rows] == ["keep-2", "keep-3"]
