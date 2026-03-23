from __future__ import annotations

import json
from typing import Any, AsyncIterator, Optional

import asyncpg

from src.models.events import BaseEvent, OptimisticConcurrencyError, StoredEvent, StreamMetadata


class EventStore:
    def __init__(self, dsn: str, upcaster_registry: Any | None = None):
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None
        self._upcaster_registry = upcaster_registry

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=10)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("EventStore is not connected. Call await connect().")
        return self._pool

    @staticmethod
    def _row_to_event(row: Any) -> StoredEvent:
        data = dict(row)
        if isinstance(data.get("payload"), str):
            data["payload"] = json.loads(data["payload"])
        if isinstance(data.get("metadata"), str):
            data["metadata"] = json.loads(data["metadata"])
        return StoredEvent(**data)

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int:
        if not events:
            return await self.stream_version(stream_id)

        pool = self._require_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                stream = await conn.fetchrow(
                    "SELECT stream_id, current_version, archived_at FROM event_streams WHERE stream_id=$1 FOR UPDATE",
                    stream_id,
                )
                if stream is None:
                    if expected_version != -1:
                        raise OptimisticConcurrencyError(stream_id, expected_version, 0)
                    aggregate_type = stream_id.split("-", 1)[0]
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version) VALUES ($1,$2,0)",
                        stream_id,
                        aggregate_type,
                    )
                    current_version = 0
                else:
                    if stream["archived_at"] is not None:
                        raise RuntimeError(f"Stream {stream_id} is archived.")
                    current_version = int(stream["current_version"])
                    if current_version != expected_version:
                        raise OptimisticConcurrencyError(stream_id, expected_version, current_version)

                next_version = current_version
                for event in events:
                    next_version += 1
                    metadata: dict[str, Any] = {"occurred_at": event.occurred_at.isoformat()}
                    if correlation_id:
                        metadata["correlation_id"] = correlation_id
                    if causation_id:
                        metadata["causation_id"] = causation_id

                    rec = await conn.fetchrow(
                        """
                        INSERT INTO events(stream_id, stream_position, event_type, event_version, payload, metadata)
                        VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb)
                        RETURNING event_id, payload
                        """,
                        stream_id,
                        next_version,
                        event.event_type,
                        event.event_version,
                        json.dumps(event.to_payload()),
                        json.dumps(metadata),
                    )
                    await conn.execute(
                        """
                        INSERT INTO outbox(event_id, destination, payload)
                        VALUES ($1, $2, $3::jsonb)
                        """,
                        rec["event_id"],
                        event.event_type,
                        json.dumps(event.to_payload()),
                    )

                await conn.execute(
                    """
                    UPDATE event_streams SET current_version=$2 WHERE stream_id=$1
                    """,
                    stream_id,
                    next_version,
                )
                return next_version

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            if to_position is None:
                rows = await conn.fetch(
                    """
                    SELECT event_id, stream_id, stream_position, global_position, event_type, event_version, payload, metadata, recorded_at
                    FROM events
                    WHERE stream_id=$1 AND stream_position > $2
                    ORDER BY stream_position ASC
                    """,
                    stream_id,
                    from_position,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT event_id, stream_id, stream_position, global_position, event_type, event_version, payload, metadata, recorded_at
                    FROM events
                    WHERE stream_id=$1 AND stream_position > $2 AND stream_position <= $3
                    ORDER BY stream_position ASC
                    """,
                    stream_id,
                    from_position,
                    to_position,
                )
            events = [self._row_to_event(row) for row in rows]
            if self._upcaster_registry is None:
                return events
            return [self._upcaster_registry.upcast(e) for e in events]

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            last = from_global_position
            while True:
                params: list[Any] = [last]
                q = """
                    SELECT event_id, stream_id, stream_position, global_position, event_type, event_version, payload, metadata, recorded_at
                    FROM events
                    WHERE global_position > $1
                """
                if event_types:
                    q += " AND event_type = ANY($2)"
                    params.append(event_types)
                    q += " ORDER BY global_position ASC LIMIT $3"
                    params.append(batch_size)
                else:
                    q += " ORDER BY global_position ASC LIMIT $2"
                    params.append(batch_size)
                rows = await conn.fetch(
                    q,
                    *params,
                )
                if not rows:
                    break
                for row in rows:
                    evt = self._row_to_event(row)
                    if self._upcaster_registry is not None:
                        evt = self._upcaster_registry.upcast(evt)
                    last = evt.global_position
                    yield evt

    async def stream_version(self, stream_id: str) -> int:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id=$1",
                stream_id,
            )
            return int(row["current_version"]) if row else 0

    async def archive_stream(self, stream_id: str) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE event_streams SET archived_at=NOW() WHERE stream_id=$1", stream_id)

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT stream_id, aggregate_type, current_version, created_at, archived_at, metadata
                FROM event_streams WHERE stream_id=$1
                """,
                stream_id,
            )
            return StreamMetadata(**dict(row)) if row else None
