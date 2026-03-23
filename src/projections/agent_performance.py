from __future__ import annotations

from src.event_store import EventStore
from src.models.events import StoredEvent


class AgentPerformanceProjection:
    name = "agent_performance"

    async def handles(self, event: StoredEvent) -> bool:
        return event.event_type in {"CreditAnalysisCompleted", "DecisionGenerated"}

    async def apply(self, event: StoredEvent, store: EventStore) -> None:
        agent_id = event.payload.get("agent_id") or event.payload.get("orchestrator_agent_id")
        if not agent_id:
            return
        model_version = event.payload.get("model_version", "unknown")
        pool = store._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_performance_ledger(agent_id, model_version, analyses_completed, decisions_generated, first_seen_at, last_seen_at)
                VALUES ($1,$2,$3,$4,$5,$5)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                  analyses_completed=agent_performance_ledger.analyses_completed + EXCLUDED.analyses_completed,
                  decisions_generated=agent_performance_ledger.decisions_generated + EXCLUDED.decisions_generated,
                  last_seen_at=EXCLUDED.last_seen_at
                """,
                agent_id,
                model_version,
                1 if event.event_type == "CreditAnalysisCompleted" else 0,
                1 if event.event_type == "DecisionGenerated" else 0,
                event.recorded_at,
            )
