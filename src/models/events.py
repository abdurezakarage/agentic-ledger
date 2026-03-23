from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class DomainError(Exception):
    def __init__(self, message: str, code: str = "DOMAIN_ERROR", details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}


class OptimisticConcurrencyError(Exception):
    def __init__(self, stream_id: UUID, expected_version: int, actual_version: int):
        super().__init__(
            f"Optimistic concurrency conflict for stream {stream_id}: "
            f"expected_version={expected_version}, actual_version={actual_version}"
        )
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version


class BaseEvent(BaseModel):
    event_type: ClassVar[str]
    event_version: int = 1
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: Optional[UUID] = None
    causation_id: Optional[UUID] = None

    def to_storable(self) -> Dict[str, Any]:
        payload = self.model_dump(exclude={"occurred_at", "correlation_id", "causation_id"})
        return {
            "event_type": self.__class__.event_type,
            "event_version": self.event_version,
            "occurred_at": self.occurred_at,
            "payload": payload,
            "metadata": {},
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
        }


class StoredEvent(BaseModel):
    event_id: UUID
    stream_id: UUID
    version: int
    event_type: str
    event_version: int
    occurred_at: datetime
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[UUID] = None
    causation_id: Optional[UUID] = None


class StreamMetadata(BaseModel):
    stream_id: UUID
    stream_type: str
    created_at: datetime
    archived_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    last_version: int = 0


# ---- Event Catalogue (minimal set for interim + full loan lifecycle replay) ----


class ApplicationSubmitted(BaseEvent):
    event_type: ClassVar[str] = "ApplicationSubmitted"
    application_id: UUID
    applicant_id: UUID
    amount: int
    term_months: int


class CreditAnalysisCompleted(BaseEvent):
    event_type: ClassVar[str] = "CreditAnalysisCompleted"
    application_id: UUID
    risk_tier: str  # LOW | MEDIUM | HIGH
    score: int


class FraudScreeningCompleted(BaseEvent):
    event_type: ClassVar[str] = "FraudScreeningCompleted"
    application_id: UUID
    status: str  # CLEAR | FLAGGED


class ComplianceCheckCompleted(BaseEvent):
    event_type: ClassVar[str] = "ComplianceCheckCompleted"
    application_id: UUID
    status: str  # PASS | FAIL
    regulation_version: str = "v1"


class DecisionGenerated(BaseEvent):
    event_type: ClassVar[str] = "DecisionGenerated"
    application_id: UUID
    decision: str  # APPROVE | DECLINE
    rationale: str


class HumanReviewCompleted(BaseEvent):
    event_type: ClassVar[str] = "HumanReviewCompleted"
    application_id: UUID
    final_decision: str  # FINAL_APPROVED | FINAL_DECLINED
    reviewer_id: UUID = Field(default_factory=uuid4)


class AgentSessionStarted(BaseEvent):
    event_type: ClassVar[str] = "AgentSessionStarted"
    session_id: UUID
    agent_id: UUID
    model_version: str
    gas_town_context_id: UUID
    token_budget: int = 8000


class LoanDisbursed(BaseEvent):
    event_type: ClassVar[str] = "LoanDisbursed"
    application_id: UUID
    amount: int
    account_number: str


EVENT_TYPE_REGISTRY: Dict[str, type[BaseEvent]] = {
    ApplicationSubmitted.event_type: ApplicationSubmitted,
    CreditAnalysisCompleted.event_type: CreditAnalysisCompleted,
    FraudScreeningCompleted.event_type: FraudScreeningCompleted,
    ComplianceCheckCompleted.event_type: ComplianceCheckCompleted,
    DecisionGenerated.event_type: DecisionGenerated,
    HumanReviewCompleted.event_type: HumanReviewCompleted,
    AgentSessionStarted.event_type: AgentSessionStarted,
    LoanDisbursed.event_type: LoanDisbursed,
}


def decode_event(event_type: str, payload: Dict[str, Any], *, occurred_at: datetime) -> BaseEvent:
    cls = EVENT_TYPE_REGISTRY.get(event_type)
    if cls is None:
        raise DomainError(f"Unknown event_type: {event_type}")
    return cls(**payload, occurred_at=occurred_at)
