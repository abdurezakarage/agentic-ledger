from __future__ import annotations

from src.event_store import EventStore
from src.models.events import StoredEvent


class ApplicationSummaryProjection:
    name = "application_summary"

    async def handles(self, event: StoredEvent) -> bool:
        return event.stream_id.startswith("loan-")

    async def apply(self, event: StoredEvent, store: EventStore) -> None:
        app_id = event.payload.get("application_id") or event.stream_id.replace("loan-", "", 1)
        state = event.event_type
        pool = store._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO application_summary(application_id, state, applicant_id, requested_amount_usd, last_event_type, last_event_at)
                VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (application_id) DO UPDATE SET
                  state=EXCLUDED.state,
                  applicant_id=COALESCE(EXCLUDED.applicant_id, application_summary.applicant_id),
                  requested_amount_usd=COALESCE(EXCLUDED.requested_amount_usd, application_summary.requested_amount_usd),
                  decision=COALESCE($7, application_summary.decision),
                  human_reviewer_id=COALESCE($8, application_summary.human_reviewer_id),
                  final_decision_at=CASE WHEN $8 IS NULL THEN application_summary.final_decision_at ELSE EXCLUDED.last_event_at END,
                  last_event_type=EXCLUDED.last_event_type,
                  last_event_at=EXCLUDED.last_event_at
                """,
                app_id,
                state,
                event.payload.get("applicant_id"),
                event.payload.get("requested_amount_usd"),
                event.event_type,
                event.recorded_at,
                event.payload.get("recommendation") or event.payload.get("final_decision"),
                event.payload.get("reviewer_id"),
            )
