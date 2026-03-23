from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.event_store import EventStore
from src.models.events import ApplicationState, DomainError, StoredEvent


@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: Optional[ApplicationState] = None
    applicant_id: Optional[str] = None
    requested_amount_usd: Optional[float] = None
    risk_tier: Optional[str] = None
    compliance_cleared: bool = False
    has_credit_analysis: bool = False
    has_human_override: bool = False
    version: int = 0

    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
        agg = cls(application_id=application_id)
        for event in await store.load_stream(f"loan-{application_id}"):
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler is None:
            self.version = event.stream_position
            return
        handler(event)
        self.version = event.stream_position

    def assert_awaiting_credit_analysis(self) -> None:
        if self.state not in {ApplicationState.SUBMITTED, ApplicationState.AWAITING_ANALYSIS}:
            raise DomainError(f"Expected awaiting analysis, got {self.state}")

    def assert_compliance_ready_for_approval(self) -> None:
        if not self.compliance_cleared:
            raise DomainError("Cannot approve without compliance clearance")

    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = event.payload.get("applicant_id")
        self.requested_amount_usd = event.payload.get("requested_amount_usd")

    def _on_CreditAnalysisRequested(self, _: StoredEvent) -> None:
        self.state = ApplicationState.AWAITING_ANALYSIS

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        if self.has_credit_analysis and not self.has_human_override:
            raise DomainError("Credit analysis already completed and not superseded")
        self.has_credit_analysis = True
        self.risk_tier = event.payload.get("risk_tier")
        self.state = ApplicationState.ANALYSIS_COMPLETE

    def _on_ComplianceRulePassed(self, _: StoredEvent) -> None:
        self.state = ApplicationState.COMPLIANCE_REVIEW
        self.compliance_cleared = True

    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        confidence = float(event.payload.get("confidence_score", 0))
        recommendation = event.payload.get("recommendation", "REFER")
        if confidence < 0.6 and recommendation != "REFER":
            raise DomainError("Confidence floor requires REFER recommendation")
        self.state = ApplicationState.PENDING_DECISION

    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        if event.payload.get("override"):
            self.has_human_override = True
        final_decision = event.payload.get("final_decision")
        if final_decision in {"APPROVE", "FINAL_APPROVED"}:
            self.state = ApplicationState.FINAL_APPROVED
        else:
            self.state = ApplicationState.FINAL_DECLINED

