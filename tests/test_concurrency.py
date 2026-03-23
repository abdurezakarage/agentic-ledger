import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest

from src.event_store import EventStore
from src.models.events import ApplicationSubmitted, DecisionGenerated, OptimisticConcurrencyError


def _dsn() -> str | None:
    return os.getenv("DATABASE_URL")

def _schema_sql() -> str:
    schema_path = os.path.join(os.path.dirname(__file__), "..", "src", "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        return f.read()

async def _apply_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(_schema_sql())


@pytest.mark.asyncio
async def test_double_decision_concurrency_expected_version_3():
    dsn = _dsn()
    if not dsn:
        pytest.skip("DATABASE_URL not set (needs Postgres).")

    store = EventStore(dsn)
    await store.connect()
    try:
        # Ensure schema exists.
        async with asyncpg.connect(dsn) as conn:
            await _apply_schema(conn)
            # Clean tables for repeatability.
            await conn.execute("TRUNCATE TABLE outbox RESTART IDENTITY CASCADE")
            await conn.execute("TRUNCATE TABLE events RESTART IDENTITY CASCADE")
            await conn.execute("TRUNCATE TABLE event_streams RESTART IDENTITY CASCADE")

        stream_id = uuid4()

        # Seed 3 events so stream_version == 3.
        seed = [
            ApplicationSubmitted(
                application_id=stream_id,
                applicant_id=uuid4(),
                amount=10000,
                term_months=12,
            ).to_storable(),
            DecisionGenerated(application_id=stream_id, decision="APPROVE", rationale="seed-1").to_storable(),
            DecisionGenerated(application_id=stream_id, decision="APPROVE", rationale="seed-2").to_storable(),
        ]
        await store.append(stream_id=stream_id, stream_type="LoanApplication", events=seed, expected_version=0)

        assert await store.stream_version(stream_id) == 3

        async def contender(label: str):
            ev = DecisionGenerated(application_id=stream_id, decision="DECLINE", rationale=label).to_storable()
            return await store.append(
                stream_id=stream_id,
                stream_type="LoanApplication",
                events=[ev],
                expected_version=3,
            )

        results = await asyncio.gather(
            contender("agent-a"),
            contender("agent-b"),
            return_exceptions=True,
        )

        successes = [r for r in results if not isinstance(r, Exception)]
        failures = [r for r in results if isinstance(r, Exception)]

        assert len(successes) == 1
        assert len(failures) == 1
        err = failures[0]
        assert isinstance(err, OptimisticConcurrencyError)
        assert err.stream_id == stream_id
        assert err.expected_version == 3
        assert err.actual_version == 4
        assert await store.stream_version(stream_id) == 4
        assert len(await store.load_stream(stream_id)) == 4
    finally:
        await store.close()

