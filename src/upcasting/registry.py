from __future__ import annotations

from typing import Callable

from src.models.events import StoredEvent


class UpcasterRegistry:
    def __init__(self) -> None:
        self._upcasters: dict[tuple[str, int], Callable[[dict], dict]] = {}

    def register(self, event_type: str, from_version: int) -> Callable:
        def decorator(fn: Callable[[dict], dict]) -> Callable[[dict], dict]:
            self._upcasters[(event_type, from_version)] = fn
            return fn

        return decorator

    def upcast(self, event: StoredEvent) -> StoredEvent:
        current = event
        version = event.event_version
        while (event.event_type, version) in self._upcasters:
            new_payload = self._upcasters[(event.event_type, version)](current.payload)
            current = current.with_payload(new_payload, version=version + 1)
            version += 1
        return current
