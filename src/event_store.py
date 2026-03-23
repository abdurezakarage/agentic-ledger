from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Sequence
from uuid import UUID

import asyncpg

from src.models.events import OptimisticConcurrencyError, StoredEvent, StreamMetadata


@dataclass(frozen=True)
class _AppendRow:
    event_type: str
    event_version: int
    occurred_at: datetime
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
    correlation_id: Optional[UUID]
    causation_id: Optional[UUID]


class EventStore:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

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

    async def get_stream_metadata(self, stream_id: UUID) -> Optional[StreamMetadata]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT s.stream_id, s.stream_type, s.created_at, s.archived_at, s.metadata,
                       COALESCE((SELECT MAX(e.version) FROM events e WHERE e.stream_id = s.stream_id), 0) AS last_version
                FROM event_streams s
                WHERE s.stream_id = $1
                """,
                stream_id,
            )
            if row is None:
                return None
            return StreamMetadata(
                stream_id=row["stream_id"],
                stream_type=row["stream_type"],
                created_at=row["created_at"],
                archived_at=row["archived_at"],
                metadata=dict(row["metadata"] or {}),
                last_version=row["last_version"],
            )

    async def stream_version(self, stream_id: UUID) -> int:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COALESCE(MAX(version), 0) AS v FROM events WHERE stream_id = $1",
                stream_id,
            )
            return int(row["v"])

    async def archive_stream(self, stream_id: UUID) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_streams SET archived_at = now() WHERE stream_id = $1",
                stream_id,
            )

    async def append(
        self,
        *,
        stream_id: UUID,
        stream_type: str,
        events: Sequence[Dict[str, Any]],
        expected_version: int,
        stream_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[StoredEvent]:
        """
        Append events to stream enforcing optimistic concurrency via expected_version.
        expected_version is the stream version observed by the caller (0 for new stream).
        """
        if not events:
            return []

        pool = self._require_pool()
        rows: List[_AppendRow] = []
        for e in events:
            rows.append(
                _AppendRow(
                    event_type=e["event_type"],
                    event_version=int(e.get("event_version", 1)),
                    occurred_at=e["occurred_at"],
                    payload=dict(e["payload"]),
                    metadata=dict(e.get("metadata") or {}),
                    correlation_id=e.get("correlation_id"),
                    causation_id=e.get("causation_id"),
                )
            )

        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO event_streams(stream_id, stream_type, metadata)
                    VALUES ($1, $2, COALESCE($3::jsonb, '{}'::jsonb))
                    ON CONFLICT (stream_id) DO NOTHING
                    """,
                    stream_id,
                    stream_type,
                    stream_metadata,
                )

                # Lock stream row to serialize appends for this stream.
                stream_row = await conn.fetchrow(
                    "SELECT stream_id, archived_at FROM event_streams WHERE stream_id=$1",
                    stream_id,
                )
                if stream_row is None:
                    raise RuntimeError("Stream row missing after upsert.")
                if stream_row["archived_at"] is not None:
                    raise RuntimeError(f"Stream {stream_id} is archived.")

                inserted: List[StoredEvent] = []
                next_version = expected_version
                for r in rows:
                    next_version += 1
                    try:
                        rec = await conn.fetchrow(
                            """
                            INSERT INTO events(
                              stream_id, version, event_type, event_version, occurred_at,
                              payload, metadata, correlation_id, causation_id
                            )
                            VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9)
                            RETURNING event_id, stream_id, version, event_type, event_version,
                                      occurred_at, payload, metadata, correlation_id, causation_id
                            """,
                            stream_id,
                            next_version,
                            r.event_type,
                            r.event_version,
                            r.occurred_at,
                            r.payload,
                            r.metadata,
                            r.correlation_id,
                            r.causation_id,
                        )
                        await conn.execute(
                            """
                            INSERT INTO outbox (event_id, stream_id, topic, payload, occurred_at)
                            VALUES ($1, $2, $3, $4::jsonb, $5)
                            """,
                            rec["event_id"],
                            rec["stream_id"],
                            rec["event_type"],
                            rec["payload"],
                            rec["occurred_at"],
                        )
                    except asyncpg.exceptions.UniqueViolationError as e:
                        if e.constraint_name == "events_stream_version_unique":
                            actual_version_row = await conn.fetchrow(
                                "SELECT COALESCE(MAX(version), 0) AS v FROM events WHERE stream_id=$1",
                                stream_id,
                            )
                            actual_version = int(actual_version_row["v"]) if actual_version_row else 0
                            raise OptimisticConcurrencyError(stream_id, expected_version, actual_version) from e
                        raise
                        
                    inserted.append(
                        StoredEvent(
                            event_id=rec["event_id"],
                            stream_id=rec["stream_id"],
                            version=rec["version"],
                            event_type=rec["event_type"],
                            event_version=rec["event_version"],
                            occurred_at=rec["occurred_at"],
                            payload=dict(rec["payload"]),
                            metadata=dict(rec["metadata"] or {}),
                            correlation_id=rec["correlation_id"],
                            causation_id=rec["causation_id"],
                        )
                    )
                return inserted

    async def load_stream(self, stream_id: UUID, *, from_version: int = 1) -> List[StoredEvent]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_id, stream_id, version, event_type, event_version, occurred_at,
                       payload, metadata, correlation_id, causation_id
                FROM events
                WHERE stream_id = $1 AND version >= $2
                ORDER BY version ASC
                """,
                stream_id,
                from_version,
            )
            return [
                StoredEvent(
                    event_id=r["event_id"],
                    stream_id=r["stream_id"],
                    version=r["version"],
                    event_type=r["event_type"],
                    event_version=r["event_version"],
                    occurred_at=r["occurred_at"],
                    payload=dict(r["payload"]),
                    metadata=dict(r["metadata"] or {}),
                    correlation_id=r["correlation_id"],
                    causation_id=r["causation_id"],
                )
                for r in rows
            ]

    async def load_all(
        self,
        *,
        since: Optional[datetime] = None,
        limit: int = 1000,
    ) -> List[StoredEvent]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            if since is None:
                rows = await conn.fetch(
                    """
                    SELECT event_id, stream_id, version, event_type, event_version, occurred_at,
                           payload, metadata, correlation_id, causation_id
                    FROM events
                    ORDER BY occurred_at ASC
                    LIMIT $1
                    """,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT event_id, stream_id, version, event_type, event_version, occurred_at,
                           payload, metadata, correlation_id, causation_id
                    FROM events
                    WHERE occurred_at >= $1
                    ORDER BY occurred_at ASC
                    LIMIT $2
                    """,
                    since,
                    limit,
                )
            return [
                StoredEvent(
                    event_id=r["event_id"],
                    stream_id=r["stream_id"],
                    version=r["version"],
                    event_type=r["event_type"],
                    event_version=r["event_version"],
                    occurred_at=r["occurred_at"],
                    payload=dict(r["payload"]),
                    metadata=dict(r["metadata"] or {}),
                    correlation_id=r["correlation_id"],
                    causation_id=r["causation_id"],
                )
                for r in rows
            ]

    async def iter_all(
        self,
        *,
        batch_size: int = 1000,
    ) -> AsyncIterator[StoredEvent]:
        offset_event_time: Optional[datetime] = None
        while True:
            batch = await self.load_all(since=offset_event_time, limit=batch_size)
            if not batch:
                return
            for e in batch:
                yield e
            offset_event_time = batch[-1].occurred_at
