from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from src.commands.handlers import _append
from src.event_store import EventStore


@dataclass
class IntegrityCheckResult:
    events_verified: int
    chain_valid: bool
    tamper_detected: bool
    integrity_hash: str
    previous_hash: str


async def run_integrity_check(store: EventStore, entity_type: str, entity_id: str) -> IntegrityCheckResult:
    primary_stream = f"loan-{entity_id}" if entity_type == "application" else f"{entity_type}-{entity_id}"
    events = await store.load_stream(primary_stream)
    audit_stream = f"audit-{entity_type}-{entity_id}"
    prior = await store.load_stream(audit_stream)
    previous_hash = prior[-1].payload.get("integrity_hash", "") if prior else ""
    previous_count = int(prior[-1].payload.get("events_verified_count", 0)) if prior else 0
    digest = hashlib.sha256()
    digest.update(previous_hash.encode("utf-8"))
    for event in events:
        digest.update(json.dumps(event.payload, sort_keys=True).encode("utf-8"))
    integrity_hash = digest.hexdigest()
    chain_valid = True
    tamper_detected = False
    if prior:
        # If the stream grew but the previous hash no longer matches what we'd compute for the
        # previously verified prefix, we treat it as tampering.
        prefix = events[:previous_count]
        d2 = hashlib.sha256()
        d2.update((prior[-1].payload.get("previous_hash") or "").encode("utf-8"))
        for event in prefix:
            d2.update(json.dumps(event.payload, sort_keys=True).encode("utf-8"))
        expected_prev_hash = d2.hexdigest()
        if expected_prev_hash != previous_hash:
            chain_valid = False
            tamper_detected = True
    await _append(
        store,
        audit_stream,
        "AuditIntegrityCheckRun",
        {
            "entity_id": entity_id,
            "events_verified_count": len(events),
            "integrity_hash": integrity_hash,
            "previous_hash": previous_hash,
        },
        len(prior) if prior else -1,
    )
    return IntegrityCheckResult(
        events_verified=len(events),
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
        integrity_hash=integrity_hash,
        previous_hash=previous_hash,
    )
