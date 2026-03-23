import os

import asyncpg
import pytest

from src.event_store import EventStore
from src.models.events import BaseEvent
from src.upcasting.upcasters import registry


@pytest.mark.asyncio
async def test_upcasting_does_not_mutate_stored_payload():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")

    class CreditV1(BaseEvent):
        event_type = "CreditAnalysisCompleted"
        application_id: str
        decision: str
        reason: str

    store = EventStore(dsn, upcaster_registry=registry)
    await store.connect()
    try:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(open("src/schema.sql", "r", encoding="utf-8").read())
            await conn.execute("TRUNCATE TABLE outbox, events, event_streams RESTART IDENTITY CASCADE")
        finally:
            await conn.close()
        await store.append("loan-upcast-test", [CreditV1(application_id="a1", decision="APPROVE", reason="legacy")], -1)
        conn = await asyncpg.connect(dsn)
        try:
            raw_before = await conn.fetchrow(
                "SELECT payload FROM events WHERE stream_id=$1 AND stream_position=1", "loan-upcast-test"
            )
        finally:
            await conn.close()
        loaded = await store.load_stream("loan-upcast-test")
        assert loaded[0].event_version >= 1
        conn = await asyncpg.connect(dsn)
        try:
            raw_after = await conn.fetchrow(
                "SELECT payload FROM events WHERE stream_id=$1 AND stream_position=1", "loan-upcast-test"
            )
        finally:
            await conn.close()
        assert raw_before["payload"] == raw_after["payload"]
    finally:
        await store.close()
