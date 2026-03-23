-- TRP1 Week 5 (Interim) — Minimal Ledger schema
-- Tables: event_streams, events, projection_checkpoints, outbox

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS event_streams (
  stream_id UUID PRIMARY KEY,
  stream_type TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  archived_at TIMESTAMPTZ NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_event_streams_type ON event_streams(stream_type);
CREATE INDEX IF NOT EXISTS idx_event_streams_archived_at ON event_streams(archived_at);

CREATE TABLE IF NOT EXISTS events (
  global_position BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
  event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  stream_id UUID NOT NULL REFERENCES event_streams(stream_id) ON DELETE RESTRICT,
  version INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  event_version INTEGER NOT NULL DEFAULT 1,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload JSONB NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  correlation_id UUID NULL,
  causation_id UUID NULL,
  CONSTRAINT events_stream_version_unique UNIQUE(stream_id, version)
);


CREATE INDEX IF NOT EXISTS idx_events_stream_id_version ON events(stream_id, version);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON events(occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_correlation_id ON events(correlation_id);

CREATE TABLE IF NOT EXISTS projection_checkpoints (
  projection_name TEXT PRIMARY KEY,
  last_position BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
  outbox_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_id UUID NULL REFERENCES events(event_id) ON DELETE SET NULL,
  stream_id UUID NULL,
  topic TEXT NOT NULL,
  payload JSONB NOT NULL,
  processed_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_processed_at ON outbox(processed_at);
