from __future__ import annotations

from datetime import datetime

from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.mcp.resources import get_application, get_application_compliance


async def generate_regulatory_package(store: EventStore, application_id: str, examination_date: str) -> dict:
    exam_dt = datetime.fromisoformat(examination_date)
    loan_events = await store.load_stream(f"loan-{application_id}")
    compliance_as_of = await get_application_compliance(store, application_id, as_of=exam_dt.isoformat())
    application_summary = await get_application(store, application_id)
    integrity = await run_integrity_check(store, "application", application_id)
    narrative = [f"{e.recorded_at.isoformat()} - {e.event_type}" for e in loan_events]
    return {
        "application_id": application_id,
        "event_stream": [e.model_dump() for e in loan_events],
        "projection_state_at_examination": {
            "application": application_summary,
            "compliance": compliance_as_of,
        },
        "integrity_verification": {
            "events_verified": integrity.events_verified,
            "chain_valid": integrity.chain_valid,
            "tamper_detected": integrity.tamper_detected,
            "integrity_hash": integrity.integrity_hash,
        },
        "narrative": narrative,
    }
