from __future__ import annotations

from dataclasses import dataclass

from src.event_store import EventStore
from src.models.events import BaseEvent, StoredEvent


@dataclass
class WhatIfResult:
    real_outcome: dict
    counterfactual_outcome: dict
    divergence_events: list[str]


async def run_what_if(
    store: EventStore,
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list[BaseEvent],
    projections: list,
) -> WhatIfResult:
    real_events = await store.load_stream(f"loan-{application_id}")
    branched: list[StoredEvent] = []
    injected = False
    for event in real_events:
        if not injected and event.event_type == branch_at_event_type:
            injected = True
            for c in counterfactual_events:
                branched.append(
                    event.with_payload(c.to_payload(), version=c.event_version)
                )
            continue
        branched.append(event)
    return WhatIfResult(
        real_outcome={"event_count": len(real_events)},
        counterfactual_outcome={"event_count": len(branched)},
        divergence_events=[branch_at_event_type],
    )
