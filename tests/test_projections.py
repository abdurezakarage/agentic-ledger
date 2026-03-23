import os

import asyncpg
import pytest

from src.event_store import EventStore
from src.models.events import BaseEvent
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.projections.daemon import ProjectionDaemon


class E(BaseEvent):
    event_type: str = "ApplicationSubmitted"
    application_id: str
    applicant_id: str
    requested_amount_usd: float


@pytest.mark.asyncio
async def test_projection_daemon_processes_events():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    store = EventStore(dsn)
    await store.connect()
    try:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(open("src/schema.sql", "r", encoding="utf-8").read())
            await conn.execute("TRUNCATE TABLE outbox, events, event_streams, projection_checkpoints, application_summary, compliance_audit_view RESTART IDENTITY CASCADE")
        finally:
            await conn.close()
        await store.append("loan-p1", [E(application_id="p1", applicant_id="u1", requested_amount_usd=10)], -1)
        daemon = ProjectionDaemon(store, [ApplicationSummaryProjection(), ComplianceAuditProjection()])
        await daemon._process_batch()
        assert daemon.get_lag("application_summary") >= 0
    finally:
        await store.close()
