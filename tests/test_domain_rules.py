import os

import asyncpg
import pytest

from src.commands.handlers import (
    handle_compliance_check,
    CreditAnalysisCompletedCommand,
    SubmitApplicationCommand,
    handle_credit_analysis_completed,
    handle_generate_decision,
    handle_start_agent_session,
    handle_submit_application,
)
from src.aggregates.agent_session import AgentSessionAggregate
from src.event_store import EventStore
from src.models.events import DomainError, GenericEvent


@pytest.mark.asyncio
async def test_confidence_floor_enforced_to_refer():
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
        await handle_submit_application(
            SubmitApplicationCommand(application_id="app-r1", applicant_id="u1", requested_amount_usd=100), store
        )
        await handle_start_agent_session(store, "a1", "s1", "replay", "v1")
        await handle_credit_analysis_completed(
            CreditAnalysisCompletedCommand(
                application_id="app-r1",
                agent_id="a1",
                session_id="s1",
                model_version="v1",
                confidence_score=0.91,
                risk_tier="MEDIUM",
                recommended_limit_usd=100,
                analysis_duration_ms=12,
                input_data_hash="h",
            ),
            store,
        )
        await handle_compliance_check(store, "app-r1", "KYC", "v1", True)
        await handle_generate_decision(store, "app-r1", "o1", "APPROVE", 0.2, ["agent-a1-s1"])
        events = await store.load_stream("loan-app-r1")
        assert events[-1].payload["recommendation"] == "REFER"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_invalid_contributing_session_rejected():
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
        await handle_submit_application(
            SubmitApplicationCommand(application_id="app-r2", applicant_id="u2", requested_amount_usd=100), store
        )
        await handle_start_agent_session(store, "a2", "s2", "replay", "v1")
        with pytest.raises(DomainError):
            await handle_generate_decision(store, "app-r2", "o1", "REFER", 0.8, ["agent-a2-s2"])
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_agent_first_event_must_be_context_loaded():
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
        await store.append(
            "agent-a3-s3",
            [
                GenericEvent(
                    event_type="CreditAnalysisCompleted",
                    data={"application_id": "x"},
                )
            ],
            -1,
        )
        with pytest.raises(DomainError):
            await AgentSessionAggregate.load(store, "a3", "s3")
    finally:
        await store.close()
