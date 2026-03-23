from __future__ import annotations

from dataclasses import dataclass

from src.event_store import EventStore


@dataclass
class AgentContext:
    context_text: str
    last_event_position: int
    pending_work: list[str]
    session_health_status: str


async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    events = await store.load_stream(f"agent-{agent_id}-{session_id}")
    if not events:
        return AgentContext("", 0, [], "EMPTY")
    last = events[-1]
    lines = [f"{e.event_type}: {e.payload}" for e in events[-3:]]
    pending = [e.event_type for e in events if "PENDING" in e.event_type.upper() or "ERROR" in e.event_type.upper()]
    status = "NEEDS_RECONCILIATION" if "Partial" in last.event_type else "HEALTHY"
    text = "\n".join(lines)
    if len(text) > token_budget:
        text = text[:token_budget]
    return AgentContext(
        context_text=text,
        last_event_position=last.stream_position,
        pending_work=pending,
        session_health_status=status,
    )
