from __future__ import annotations

from datetime import datetime

from src.event_store import EventStore
from src.projections.compliance_audit import ComplianceAuditProjection


async def get_application(store: EventStore, application_id: str) -> dict:
    pool = store._require_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM application_summary WHERE application_id=$1", application_id)
        return dict(row) if row else {}


async def get_application_compliance(store: EventStore, application_id: str, as_of: str | None = None) -> dict:
    projection = ComplianceAuditProjection()
    if as_of:
        return await projection.get_compliance_at(store, application_id, datetime.fromisoformat(as_of))
    return await projection.get_current_compliance(store, application_id)


async def get_application_audit_trail(store: EventStore, application_id: str, from_pos: int = 0, to_pos: int | None = None) -> list[dict]:
    events = await store.load_stream(f"audit-application-{application_id}", from_pos, to_pos)
    return [e.model_dump() for e in events]


async def get_agent_performance(store: EventStore, agent_id: str) -> list[dict]:
    pool = store._require_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM agent_performance_ledger WHERE agent_id=$1", agent_id)
        return [dict(r) for r in rows]


async def get_agent_session(store: EventStore, agent_id: str, session_id: str) -> list[dict]:
    events = await store.load_stream(f"agent-{agent_id}-{session_id}")
    return [e.model_dump() for e in events]


async def get_ledger_health(store: EventStore, lags: dict[str, int]) -> dict:
    return {"lags": lags}
