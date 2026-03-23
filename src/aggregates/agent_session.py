from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.event_store import EventStore
from src.models.events import DomainError, StoredEvent


@dataclass
class AgentSessionAggregate:
    agent_id: str
    session_id: str
    model_version: Optional[str] = None
    context_loaded: bool = False
    applications_seen: set[str] | None = None
    version: int = 0

    def __post_init__(self) -> None:
        if self.applications_seen is None:
            self.applications_seen = set()

    @classmethod
    async def load(cls, store: EventStore, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        agg = cls(agent_id=agent_id, session_id=session_id)
        for event in await store.load_stream(f"agent-{agent_id}-{session_id}"):
            agg._apply(event)
        return agg

    def assert_context_loaded(self) -> None:
        if not self.context_loaded:
            raise DomainError("Agent context must be loaded before decisions")

    def assert_model_version_current(self, model_version: str) -> None:
        if self.model_version and self.model_version != model_version:
            raise DomainError("Model version mismatch for locked session")

    def _apply(self, event: StoredEvent) -> None:
        if event.stream_position == 1 and event.event_type != "AgentContextLoaded":
            raise DomainError("First AgentSession event must be AgentContextLoaded")
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler is None:
            self.version = event.stream_position
            return
        handler(event)
        self.version = event.stream_position

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        self.context_loaded = True
        self.model_version = event.payload.get("model_version")

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        app_id = event.payload.get("application_id")
        if app_id:
            self.applications_seen.add(app_id)

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        app_id = event.payload.get("application_id")
        if app_id:
            self.applications_seen.add(app_id)

