## TRP1 Week 5 — Interim Deliverables (Minimum)

This repo contains the **interim** implementation of the Ledger core: Postgres schema, an async event store with **optimistic concurrency**, minimal domain aggregates, command handlers, and the required concurrency test.

### Setup

- **Python**: 3.11+
- **PostgreSQL**: 14+ recommended
- **Connection**: set `DATABASE_URL` (asyncpg DSN)

Recommended: copy `.env.example` to `.env` and edit the password/db name.

### Install (uv)

```bash
uv sync
```

### Apply schema / migrations

The test suite auto-applies `src/schema.sql` to the configured database.

If you want to apply manually (works without `psql`):

```bash
uv run python -m src.migrate
```

### Run tests

```bash
uv run pytest -q
```

### Interim deliverables map

- **Schema**: `src/schema.sql`
- **Event store**: `src/event_store.py`
- **Models & catalogue**: `src/models/events.py`
- **Aggregates**: `src/aggregates/loan_application.py`, `src/aggregates/agent_session.py`
- **Command handlers**: `src/commands/handlers.py`
- **Concurrency test**: `tests/test_concurrency.py`

