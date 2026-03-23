## TRP1 Week 5 — Final Submission Scaffold

This repository now includes a full Week 5 implementation scaffold for:
- Phase 1: event store core + optimistic concurrency + outbox
- Phase 2: aggregates and command handlers
- Phase 3: projections + async daemon
- Phase 4: upcasting + integrity + Gas Town recovery
- Phase 5: MCP command/resource layer
- Phase 6: what-if and regulatory package stubs

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

### Deliverables map

- **Core**: `src/schema.sql`, `src/event_store.py`, `src/models/events.py`
- **Aggregates**: `src/aggregates/*.py`
- **Commands**: `src/commands/handlers.py`
- **Projections**: `src/projections/*.py`
- **Upcasting**: `src/upcasting/*.py`
- **Integrity / Gas Town**: `src/integrity/*.py`
- **MCP layer**: `src/mcp/*.py`
- **Bonus**: `src/what_if/projector.py`, `src/regulatory/package.py`
- **Docs**: `DOMAIN_NOTES.md`, `DESIGN.md`
- **Tests**: `tests/test_*.py`

### Notes

- Tests are database-backed and require `DATABASE_URL`.
- If `DATABASE_URL` is missing, tests skip by design.

