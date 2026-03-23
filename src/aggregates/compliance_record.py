from __future__ import annotations

from dataclasses import dataclass, field

from src.event_store import EventStore
from src.models.events import DomainError, StoredEvent


@dataclass
class ComplianceRecordAggregate:
    application_id: str
    required_checks: set[str] = field(default_factory=set)
    passed_checks: set[str] = field(default_factory=set)
    failed_checks: set[str] = field(default_factory=set)
    version: int = 0

    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "ComplianceRecordAggregate":
        agg = cls(application_id=application_id)
        for event in await store.load_stream(f"compliance-{application_id}"):
            agg._apply(event)
        return agg

    def assert_all_mandatory_checks_passed(self) -> None:
        if not self.required_checks:
            raise DomainError("No compliance checks were requested")
        if self.required_checks - self.passed_checks:
            raise DomainError("Cannot clear compliance with missing checks")
        if self.failed_checks:
            raise DomainError("Compliance has failed checks")

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        self.required_checks = set(event.payload.get("checks_required", []))

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        self.passed_checks.add(str(event.payload.get("rule_id")))

    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        self.failed_checks.add(str(event.payload.get("rule_id")))
