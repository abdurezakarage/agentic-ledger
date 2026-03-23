import os

import asyncpg
import pytest

from src.mcp.tools import (
    generate_decision,
    record_compliance_check,
    record_credit_analysis,
    record_human_review,
    start_agent_session,
    submit_application,
)
from src.mcp.server import build_store


@pytest.mark.asyncio
async def test_mcp_lifecycle():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    store = build_store()
    await store.connect()
    try:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(open("src/schema.sql", "r", encoding="utf-8").read())
            await conn.execute("TRUNCATE TABLE outbox, events, event_streams RESTART IDENTITY CASCADE")
        finally:
            await conn.close()
        app = "app-1"
        await submit_application(store, {"application_id": app, "applicant_id": "u1", "requested_amount_usd": 1000})
        await start_agent_session(store, {"agent_id": "a1", "session_id": "s1", "context_source": "replay", "model_version": "v1"})
        await record_credit_analysis(
            store,
            {
                "application_id": app,
                "agent_id": "a1",
                "session_id": "s1",
                "model_version": "v1",
                "confidence_score": 0.9,
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": 500,
                "analysis_duration_ms": 30,
                "input_data_hash": "h1",
            },
        )
        await record_compliance_check(store, {"application_id": app, "rule_id": "r1", "rule_version": "v1", "passed": True})
        await generate_decision(
            store,
            {
                "application_id": app,
                "orchestrator_agent_id": "o1",
                "recommendation": "APPROVE",
                "confidence_score": 0.8,
                "contributing_agent_sessions": ["agent-a1-s1"],
            },
        )
        out = await record_human_review(
            store,
            {"application_id": app, "reviewer_id": "hr1", "override": False, "final_decision": "FINAL_APPROVED"},
        )
        assert "final_decision" in out
    finally:
        await store.close()
