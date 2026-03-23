from __future__ import annotations

from dataclasses import dataclass

from src.event_store import EventStore
from src.models.events import DomainError, StoredEvent


@dataclass
class AuditLedgerAggregate:
    entity_type: str
    entity_id: str
    last_correlation_id: str | None = None
    version: int = 0

    @classmethod
    async def load(cls, store: EventStore, entity_type: str, entity_id: str) -> "AuditLedgerAggregate":
        agg = cls(entity_type=entity_type, entity_id=entity_id)
        for event in await store.load_stream(f"audit-{entity_type}-{entity_id}"):
            agg._apply(event)
        return agg

    def assert_append_only(self) -> None:
        return

    def assert_causal_ordering(self, correlation_id: str | None) -> None:
        if self.last_correlation_id and correlation_id is None:
            raise DomainError("Audit chain requires correlation IDs for continuation")

    def _apply(self, event: StoredEvent) -> None:
        self.last_correlation_id = event.payload.get("correlation_id", self.last_correlation_id)
        self.version = event.stream_position
