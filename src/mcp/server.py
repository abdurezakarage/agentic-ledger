from __future__ import annotations

import os

from src.event_store import EventStore
from src.upcasting.upcasters import registry


def build_store() -> EventStore:
    dsn = os.getenv("DATABASE_URL", "")
    if not dsn:
        raise RuntimeError("DATABASE_URL is required")
    return EventStore(dsn=dsn, upcaster_registry=registry)
