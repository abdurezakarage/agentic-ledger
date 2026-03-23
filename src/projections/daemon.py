from __future__ import annotations

import asyncio
import logging
from typing import Protocol

from src.event_store import EventStore
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)


class Projection(Protocol):
    name: str

    async def handles(self, event: StoredEvent) -> bool: ...

    async def apply(self, event: StoredEvent, store: EventStore) -> None: ...


class ProjectionDaemon:
    def __init__(self, store: EventStore, projections: list[Projection], max_retries: int = 3):
        self._store = store
        self._projections = {p.name: p for p in projections}
        self._running = False
        self._max_retries = max_retries
        self._lags: dict[str, int] = {p.name: 0 for p in projections}

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        self._running = True
        while self._running:
            await self._process_batch()
            await asyncio.sleep(poll_interval_ms / 1000)

    def stop(self) -> None:
        self._running = False

    async def _get_checkpoint(self, projection_name: str) -> int:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_position FROM projection_checkpoints WHERE projection_name=$1",
                projection_name,
            )
            if row:
                return int(row["last_position"])
            await conn.execute(
                "INSERT INTO projection_checkpoints(projection_name, last_position) VALUES ($1, 0) ON CONFLICT DO NOTHING",
                projection_name,
            )
            return 0

    async def _set_checkpoint(self, projection_name: str, pos: int) -> None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO projection_checkpoints(projection_name, last_position, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (projection_name) DO UPDATE SET last_position=EXCLUDED.last_position, updated_at=NOW()
                """,
                projection_name,
                pos,
            )

    async def _latest_global_position(self) -> int:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COALESCE(MAX(global_position),0) AS p FROM events")
            return int(row["p"])

    async def _process_batch(self) -> None:
        checkpoints = {name: await self._get_checkpoint(name) for name in self._projections}
        start = min(checkpoints.values()) if checkpoints else 0
        events = [e async for e in self._store.load_all(from_global_position=start, batch_size=500)]
        if not events:
            return
        for event in events:
            for name, projection in self._projections.items():
                if event.global_position <= checkpoints[name]:
                    continue
                if not await projection.handles(event):
                    await self._set_checkpoint(name, event.global_position)
                    checkpoints[name] = event.global_position
                    continue
                for _ in range(self._max_retries):
                    try:
                        await projection.apply(event, self._store)
                        await self._set_checkpoint(name, event.global_position)
                        checkpoints[name] = event.global_position
                        break
                    except Exception:
                        logger.exception("Projection failed: %s", name)
                else:
                    await self._set_checkpoint(name, event.global_position)
                    checkpoints[name] = event.global_position
        latest = await self._latest_global_position()
        for name, pos in checkpoints.items():
            self._lags[name] = max(0, latest - pos)

    def get_lag(self, projection_name: str) -> int:
        return self._lags.get(projection_name, 0)

    def get_all_lags(self) -> dict[str, int]:
        return dict(self._lags)
