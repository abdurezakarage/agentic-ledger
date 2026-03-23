from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional
from uuid import UUID

from src.models.events import AgentSessionStarted, DomainError, StoredEvent, decode_event


@dataclass
class AgentSessionAggregate:
    session_id: UUID
    agent_id: Optional[UUID] = None
    model_version: Optional[str] = None
    gas_town_context_id: Optional[UUID] = None
    token_budget: int = 0
    started: bool = False
    version: int = 0

    def load(self, stored_events: Iterable[StoredEvent]) -> None:
        for se in stored_events:
            ev = decode_event(se.event_type, se.payload, occurred_at=se.occurred_at)
            self._apply(ev)
            self.version = se.version

    def start(self, *, agent_id: UUID, model_version: str, gas_town_context_id: UUID, token_budget: int) -> AgentSessionStarted:
        if self.started:
            raise DomainError("Session already started.")
        if not model_version:
            raise DomainError("model_version required")
        if token_budget <= 0:
            raise DomainError("token_budget must be positive")
        return AgentSessionStarted(
            session_id=self.session_id,
            agent_id=agent_id,
            model_version=model_version,
            gas_town_context_id=gas_town_context_id,
            token_budget=token_budget,
        )

    def enforce_gas_town(self, *, gas_town_context_id: UUID, model_version: str) -> None:
        if not self.started:
            raise DomainError("Session not started.")
        if self.gas_town_context_id != gas_town_context_id:
            raise DomainError("Gas Town context mismatch.")
        if self.model_version != model_version:
            raise DomainError("Model version mismatch (locked).")

    def _apply(self, event: object) -> None:
        event_type = event.__class__.event_type
        handler = getattr(self, f"_apply_{event_type.lower()}", None)
        if handler is None:
            raise DomainError(f"Missing apply handler for {event_type}")
        handler(event)

    def _apply_agentsessionstarted(self, e: AgentSessionStarted) -> None:
        self.started = True
        self.agent_id = e.agent_id
        self.model_version = e.model_version
        self.gas_town_context_id = e.gas_town_context_id
        self.token_budget = e.token_budget

