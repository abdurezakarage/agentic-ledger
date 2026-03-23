from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.commands.handlers import (
    CreditAnalysisCompletedCommand,
    SubmitApplicationCommand,
    handle_compliance_check,
    handle_credit_analysis_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_start_agent_session,
    handle_submit_application,
)
from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.models.events import OptimisticConcurrencyError


@dataclass
class ToolError:
    error_type: str
    message: str
    suggested_action: str
    context: dict[str, Any]


def _structured_error(exc: Exception) -> ToolError:
    if isinstance(exc, OptimisticConcurrencyError):
        return ToolError(
            error_type="OptimisticConcurrencyError",
            message=str(exc),
            suggested_action="reload_stream_and_retry",
            context={
                "stream_id": exc.stream_id,
                "expected_version": exc.expected_version,
                "actual_version": exc.actual_version,
            },
        )
    return ToolError(
        error_type=exc.__class__.__name__,
        message=str(exc),
        suggested_action="inspect_preconditions",
        context={},
    )


async def submit_application(store: EventStore, payload: dict) -> dict:
    try:
        cmd = SubmitApplicationCommand(**payload)
        await handle_submit_application(cmd, store)
        return {"stream_id": f"loan-{cmd.application_id}", "initial_version": 1}
    except Exception as exc:
        return {"error": _structured_error(exc).__dict__}


async def start_agent_session(store: EventStore, payload: dict) -> dict:
    try:
        await handle_start_agent_session(store, **payload)
        return {"session_id": payload["session_id"], "context_position": 1}
    except Exception as exc:
        return {"error": _structured_error(exc).__dict__}


async def record_credit_analysis(store: EventStore, payload: dict) -> dict:
    try:
        cmd = CreditAnalysisCompletedCommand(**payload)
        new_version = await handle_credit_analysis_completed(cmd, store)
        return {"new_stream_version": new_version}
    except Exception as exc:
        return {"error": _structured_error(exc).__dict__}


async def record_fraud_screening(store: EventStore, payload: dict) -> dict:
    return {"event_id": "not-implemented", "new_stream_version": await store.stream_version(f"loan-{payload['application_id']}")}


async def record_compliance_check(store: EventStore, payload: dict) -> dict:
    try:
        await handle_compliance_check(store, **payload)
        return {"check_id": payload["rule_id"], "compliance_status": "PASSED" if payload.get("passed") else "FAILED"}
    except Exception as exc:
        return {"error": _structured_error(exc).__dict__}


async def generate_decision(store: EventStore, payload: dict) -> dict:
    try:
        await handle_generate_decision(store, **payload)
        return {"decision_id": payload["application_id"], "recommendation": payload.get("recommendation")}
    except Exception as exc:
        return {"error": _structured_error(exc).__dict__}


async def record_human_review(store: EventStore, payload: dict) -> dict:
    try:
        await handle_human_review_completed(store, **payload)
        return {"final_decision": payload["final_decision"], "application_state": payload["final_decision"]}
    except Exception as exc:
        return {"error": _structured_error(exc).__dict__}


async def run_integrity_check_tool(store: EventStore, payload: dict) -> dict:
    try:
        result = await run_integrity_check(store, payload["entity_type"], payload["entity_id"])
        return {"check_result": result.integrity_hash, "chain_valid": result.chain_valid}
    except Exception as exc:
        return {"error": _structured_error(exc).__dict__}
