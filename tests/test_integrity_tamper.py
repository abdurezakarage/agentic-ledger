import os

import asyncpg
import pytest

from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.models.events import GenericEvent


@pytest.mark.asyncio
async def test_tamper_detection_after_db_modification():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    store = EventStore(dsn)
    await store.connect()
    try:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(open("src/schema.sql", "r", encoding="utf-8").read())
            await conn.execute("TRUNCATE TABLE outbox, events, event_streams RESTART IDENTITY CASCADE")
        finally:
            await conn.close()

        # Seed 2 events on a loan stream and run integrity check once.
        await store.append(
            "loan-tamper-1",
            [
                GenericEvent(event_type="ApplicationSubmitted", data={"application_id": "tamper-1"}),
                GenericEvent(event_type="DecisionGenerated", data={"application_id": "tamper-1", "confidence_score": 0.9, "recommendation": "REFER"}),
            ],
            -1,
        )
        r1 = await run_integrity_check(store, "application", "tamper-1")
        assert r1.chain_valid is True

        # Tamper with the first event payload directly in DB.
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(
                "UPDATE events SET payload=$1::jsonb WHERE stream_id=$2 AND stream_position=1",
                '{"application_id": "tamper-1", "tampered": true}',
                "loan-tamper-1",
            )
        finally:
            await conn.close()

        r2 = await run_integrity_check(store, "application", "tamper-1")
        assert r2.tamper_detected is True
        assert r2.chain_valid is False
    finally:
        await store.close()

