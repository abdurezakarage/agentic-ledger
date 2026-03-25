import asyncio
import os

import asyncpg
import pytest

from src.commands.handlers import SubmitApplicationCommand, handle_submit_application
from src.event_store import EventStore
from src.models.events import BaseEvent
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.projections.daemon import ProjectionDaemon


@pytest.mark.asyncio
async def test_projection_lag_under_concurrent_writes():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    store = EventStore(dsn)
    await store.connect()
    try:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(open("src/schema.sql", "r", encoding="utf-8").read())
            await conn.execute(
                "TRUNCATE TABLE outbox, events, event_streams, projection_checkpoints, application_summary, compliance_audit_view RESTART IDENTITY CASCADE"
            )
        finally:
            await conn.close()

        async def writer(i: int) -> None:
            await handle_submit_application(
                SubmitApplicationCommand(application_id=f"app-slo-{i}", applicant_id="u", requested_amount_usd=10),
                store,
            )

        await asyncio.gather(*[writer(i) for i in range(50)])

        daemon = ProjectionDaemon(store, [ApplicationSummaryProjection(), ComplianceAuditProjection()])
        await daemon._process_batch(poll_interval_ms=100)

        # Contract-style assertion: lag should be bounded in "normal operation".
        assert daemon.get_lag("application_summary") < 5000
    finally:
        await store.close()

