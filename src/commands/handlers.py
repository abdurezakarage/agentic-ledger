from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.loan_application import LoanApplicationAggregate
from src.event_store import EventStore
from src.models.events import DomainError, GenericEvent

@dataclass(frozen=True)
class SubmitApplicationCommand:
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str = "general"
    submission_channel: str = "api"
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass(frozen=True)
class CreditAnalysisCompletedCommand:
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float
    risk_tier: str
    recommended_limit_usd: float
    analysis_duration_ms: int
    input_data_hash: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


async def _append(store: EventStore, stream_id: str, event_type: str, payload: dict, expected_version: int) -> int:
    event = GenericEvent(event_type=event_type, data=payload)
    return await store.append(stream_id=stream_id, events=[event], expected_version=expected_version)


async def handle_submit_application(cmd: SubmitApplicationCommand, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    payload = {
        "application_id": cmd.application_id,
        "applicant_id": cmd.applicant_id,
        "requested_amount_usd": cmd.requested_amount_usd,
        "loan_purpose": cmd.loan_purpose,
        "submission_channel": cmd.submission_channel,
        "submitted_at": "now",
    }
    await _append(store, f"loan-{cmd.application_id}", "ApplicationSubmitted", payload, app.version if app.version else -1)


async def handle_credit_analysis_completed(cmd: CreditAnalysisCompletedCommand, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)
    app.assert_awaiting_credit_analysis()
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)
    payload = {
        "application_id": cmd.application_id,
        "agent_id": cmd.agent_id,
        "session_id": cmd.session_id,
        "model_version": cmd.model_version,
        "confidence_score": cmd.confidence_score,
        "risk_tier": cmd.risk_tier,
        "recommended_limit_usd": cmd.recommended_limit_usd,
        "analysis_duration_ms": cmd.analysis_duration_ms,
        "input_data_hash": cmd.input_data_hash,
    }
    await _append(store, f"loan-{cmd.application_id}", "CreditAnalysisCompleted", payload, app.version)
    await _append(
        store,
        f"agent-{cmd.agent_id}-{cmd.session_id}",
        "CreditAnalysisCompleted",
        payload,
        agent.version,
    )


async def handle_start_agent_session(
    store: EventStore, agent_id: str, session_id: str, context_source: str, model_version: str
) -> None:
    stream_id = f"agent-{agent_id}-{session_id}"
    expected = await store.stream_version(stream_id)
    expected = -1 if expected == 0 else expected
    await _append(
        store,
        stream_id,
        "AgentContextLoaded",
        {
            "agent_id": agent_id,
            "session_id": session_id,
            "context_source": context_source,
            "event_replay_from_position": 0,
            "context_token_count": 0,
            "model_version": model_version,
        },
        expected,
    )


async def handle_compliance_check(
    store: EventStore, application_id: str, rule_id: str, rule_version: str, passed: bool
) -> None:
    agg = await ComplianceRecordAggregate.load(store, application_id)
    if agg.version == 0:
        await _append(
            store,
            f"compliance-{application_id}",
            "ComplianceCheckRequested",
            {
                "application_id": application_id,
                "regulation_set_version": rule_version,
                "checks_required": [rule_id],
            },
            -1,
        )
        agg = await ComplianceRecordAggregate.load(store, application_id)
    event_type = "ComplianceRulePassed" if passed else "ComplianceRuleFailed"
    payload = {
        "application_id": application_id,
        "rule_id": rule_id,
        "rule_version": rule_version,
    }
    await _append(store, f"compliance-{application_id}", event_type, payload, agg.version if agg.version else -1)
    # Mirror compliance signal on the loan stream so the LoanApplication aggregate can enforce dependency rules
    app = await LoanApplicationAggregate.load(store, application_id)
    if passed:
        await _append(store, f"loan-{application_id}", "ComplianceRulePassed", payload, app.version)


async def handle_generate_decision(
    store: EventStore,
    application_id: str,
    orchestrator_agent_id: str,
    recommendation: str,
    confidence_score: float,
    contributing_agent_sessions: list[str],
) -> None:
    app = await LoanApplicationAggregate.load(store, application_id)
    if recommendation == "APPROVE":
        compliance = await ComplianceRecordAggregate.load(store, application_id)
        compliance.assert_all_mandatory_checks_passed()
        app.assert_compliance_ready_for_approval()
    for session_stream in contributing_agent_sessions:
        parts = session_stream.split("-")
        if len(parts) < 3:
            raise DomainError("Invalid contributing_agent_sessions stream id")
        agent_id = parts[1]
        session_id = "-".join(parts[2:])
        session = await AgentSessionAggregate.load(store, agent_id, session_id)
        session.assert_processed_application(application_id)
    recommendation = app.enforce_confidence_floor(recommendation, confidence_score)
    await _append(
        store,
        f"loan-{application_id}",
        "DecisionGenerated",
        {
            "application_id": application_id,
            "orchestrator_agent_id": orchestrator_agent_id,
            "recommendation": recommendation,
            "confidence_score": confidence_score,
            "contributing_agent_sessions": contributing_agent_sessions,
            "decision_basis_summary": "generated by orchestrator",
            "model_versions": {},
        },
        app.version,
    )


async def handle_human_review_completed(
    store: EventStore, application_id: str, reviewer_id: str, override: bool, final_decision: str
) -> None:
    app = await LoanApplicationAggregate.load(store, application_id)
    if override and not final_decision:
        raise DomainError("override_reason/final_decision required when override=True")
    await _append(
        store,
        f"loan-{application_id}",
        "HumanReviewCompleted",
        {
            "application_id": application_id,
            "reviewer_id": reviewer_id,
            "override": override,
            "final_decision": final_decision,
        },
        app.version,
    )
