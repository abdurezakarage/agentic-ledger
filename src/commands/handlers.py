from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from src.aggregates.loan_application import LoanApplicationAggregate
from src.event_store import EventStore


from src.aggregates.agent_session import AgentSessionAggregate

@dataclass(frozen=True)
class SubmitApplicationCommand:
    application_id: UUID
    applicant_id: UUID
    amount: int
    term_months: int
    correlation_id: Optional[UUID] = None
    causation_id: Optional[UUID] = None


@dataclass(frozen=True)
class CreditAnalysisCompletedCommand:
    application_id: UUID
    risk_tier: str
    score: int
    agent_session_id: UUID
    model_version: str
    gas_town_context_id: UUID
    correlation_id: Optional[UUID] = None
    causation_id: Optional[UUID] = None


async def handle_submit_application(store: EventStore, cmd: SubmitApplicationCommand) -> None:
    stored = await store.load_stream(cmd.application_id)
    agg = LoanApplicationAggregate(application_id=cmd.application_id)
    agg.load(stored)

    ev = agg.submit_application(applicant_id=cmd.applicant_id, amount=cmd.amount, term_months=cmd.term_months)
    if cmd.correlation_id is not None:
        ev.correlation_id = cmd.correlation_id
    if cmd.causation_id is not None:
        ev.causation_id = cmd.causation_id

    await store.append(
        stream_id=cmd.application_id,
        stream_type="LoanApplication",
        events=[ev.to_storable()],
        expected_version=agg.version,
    )


async def handle_credit_analysis_completed(store: EventStore, cmd: CreditAnalysisCompletedCommand) -> None:
    stored = await store.load_stream(cmd.application_id)
    agg = LoanApplicationAggregate(application_id=cmd.application_id)
    agg.load(stored)

    agent_stored = await store.load_stream(cmd.agent_session_id)
    agent_agg = AgentSessionAggregate(session_id=cmd.agent_session_id)
    agent_agg.load(agent_stored)

    agent_agg.enforce_gas_town(gas_town_context_id=cmd.gas_town_context_id, model_version=cmd.model_version)

    ev = agg.record_credit_analysis(risk_tier=cmd.risk_tier, score=cmd.score)
    if cmd.correlation_id is not None:
        ev.correlation_id = cmd.correlation_id
    if cmd.causation_id is not None:
        ev.causation_id = cmd.causation_id

    await store.append(
        stream_id=cmd.application_id,
        stream_type="LoanApplication",
        events=[ev.to_storable()],
        expected_version=agg.version,
    )


