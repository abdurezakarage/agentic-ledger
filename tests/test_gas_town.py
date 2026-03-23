import os

import asyncpg
import pytest

from src.event_store import EventStore
from src.integrity.gas_town import reconstruct_agent_context
from src.models.events import BaseEvent


class AgentEvent(BaseEvent):
    event_type = "AgentWorkStep"
    agent_id: str
    session_id: str
    step: str


@pytest.mark.asyncio
async def test_gas_town_context_reconstruction():
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
        sid = "agent-123-1"
        await store.append(sid, [AgentEvent(agent_id="123", session_id="1", step=f"s{i}") for i in range(5)], -1)
        context = await reconstruct_agent_context(store, "123", "1")
        assert context.last_event_position == 5
        assert context.context_text
    finally:
        await store.close()
