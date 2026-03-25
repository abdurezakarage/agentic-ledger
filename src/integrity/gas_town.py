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
    preserved = events[-3:]
    preserved_lines = [f"{e.event_type}: {e.payload}" for e in preserved]
    pending = [e.event_type for e in events if "PENDING" in e.event_type.upper() or "ERROR" in e.event_type.upper()]

    older = events[:-3]
    # Token-efficient prose summary of older events (one line each).
    summary_lines = [f"- {e.event_type}" for e in older]
    text = "\n".join(["Summary:"] + summary_lines + ["", "Last events:"] + preserved_lines).strip()

    # Partial state detection heuristic: a decision event without a corresponding completion event after it.
    last_is_partial = last.event_type in {"DecisionGenerated"} and not any(
        e.event_type in {"HumanReviewCompleted", "ApplicationApproved", "ApplicationDeclined"} for e in preserved
    )
    status = "NEEDS_RECONCILIATION" if last_is_partial else "HEALTHY"
    if len(text) > token_budget:
        text = text[:token_budget]
    return AgentContext(
        context_text=text,
        last_event_position=last.stream_position,
        pending_work=pending,
        session_health_status=status,
    )
