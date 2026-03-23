from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class DomainError(Exception):
    pass


class OptimisticConcurrencyError(Exception):
    def __init__(self, stream_id: str, expected_version: int, actual_version: int):
        super().__init__(
            f"Optimistic concurrency conflict for stream {stream_id}: "
            f"expected={expected_version}, actual={actual_version}"
        )
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version


class ApplicationState(str, Enum):
    SUBMITTED = "Submitted"
    AWAITING_ANALYSIS = "AwaitingAnalysis"
    ANALYSIS_COMPLETE = "AnalysisComplete"
    COMPLIANCE_REVIEW = "ComplianceReview"
    PENDING_DECISION = "PendingDecision"
    APPROVED_PENDING_HUMAN = "ApprovedPendingHuman"
    DECLINED_PENDING_HUMAN = "DeclinedPendingHuman"
    FINAL_APPROVED = "FinalApproved"
    FINAL_DECLINED = "FinalDeclined"


class BaseEvent(BaseModel):
    event_type: str
    event_version: int = 1
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_payload(self) -> Dict[str, Any]:
        return self.model_dump(exclude={"event_version", "occurred_at"})


class StoredEvent(BaseModel):
    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    recorded_at: datetime

    def with_payload(self, payload: Dict[str, Any], *, version: int) -> "StoredEvent":
        return self.model_copy(update={"payload": payload, "event_version": version})


class StreamMetadata(BaseModel):
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class GenericEvent(BaseEvent):
    event_type: str = "GenericEvent"
    data: Dict[str, Any] = Field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        return dict(self.data)
