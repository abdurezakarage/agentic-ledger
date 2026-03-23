from __future__ import annotations

from datetime import datetime

from src.event_store import EventStore
from src.models.events import StoredEvent


class ComplianceAuditProjection:
    name = "compliance_audit"

    async def handles(self, event: StoredEvent) -> bool:
        return event.stream_id.startswith("compliance-")

    async def apply(self, event: StoredEvent, store: EventStore) -> None:
        app_id = event.payload.get("application_id") or event.stream_id.replace("compliance-", "", 1)
        pool = store._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO compliance_audit_view(application_id, as_of, data) VALUES ($1,$2,$3::jsonb)",
                app_id,
                event.recorded_at,
                {"event_type": event.event_type, "payload": event.payload, "recorded_at": event.recorded_at.isoformat()},
            )

    async def get_current_compliance(self, store: EventStore, application_id: str) -> dict:
        pool = store._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT data FROM compliance_audit_view
                WHERE application_id=$1
                ORDER BY as_of DESC
                LIMIT 1
                """,
                application_id,
            )
            return dict(row["data"]) if row else {}

    async def get_compliance_at(self, store: EventStore, application_id: str, timestamp: datetime) -> dict:
        pool = store._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT data FROM compliance_audit_view
                WHERE application_id=$1 AND as_of <= $2
                ORDER BY as_of DESC
                LIMIT 1
                """,
                application_id,
                timestamp,
            )
            return dict(row["data"]) if row else {}

    async def rebuild_from_scratch(self, store: EventStore) -> None:
        pool = store._require_pool()
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE compliance_audit_view")
        async for event in store.load_all(from_global_position=0):
            if await self.handles(event):
                await self.apply(event, store)
