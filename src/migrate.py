from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import asyncpg
from dotenv import load_dotenv


def _schema_sql() -> str:
    schema_path = Path(__file__).resolve().parent / "schema.sql"
    return schema_path.read_text(encoding="utf-8")

def _strip_optional_quotes(value: str) -> str:
    v = value.strip()
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        return v[1:-1]
    return v


def _target_db_from_dsn(dsn: str) -> str:
    p = urlparse(dsn)
    db = (p.path or "").lstrip("/")
    return db


def _dsn_with_db(dsn: str, dbname: str) -> str:
    p = urlparse(dsn)
    return urlunparse(p._replace(path=f"/{dbname}"))


def _safe_ident(name: str) -> str:
    # Minimal safety: allow letters, digits, underscore.
    if not name or any((not (c.isalnum() or c == "_")) for c in name):
        raise SystemExit(
            f"Refusing to create database with unsafe name: {name!r}. "
            "Use only letters, digits, underscore."
        )
    return name


async def main() -> None:
    # Load .env if present. python-dotenv warns if file has invalid lines.
    load_dotenv(override=True)
    raw = os.getenv("DATABASE_URL")
    dsn = _strip_optional_quotes(raw) if raw else None
    if not dsn:
        raise SystemExit("DATABASE_URL is not set. Copy .env.example -> .env and edit it.")

    target_db = _target_db_from_dsn(dsn)
    if not target_db:
        raise SystemExit("DATABASE_URL must include a database name, e.g. ...:5432/trp1_ledger")

    try:
        conn = await asyncpg.connect(dsn)
    except asyncpg.InvalidCatalogNameError:
        # Database doesn't exist yet — connect to default `postgres` db and create it.
        admin_dsn = _dsn_with_db(dsn, "postgres")
        admin = await asyncpg.connect(admin_dsn)
        try:
            dbname = _safe_ident(target_db)
            exists = await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", dbname)
            if not exists:
                await admin.execute(f'CREATE DATABASE "{dbname}"')
        finally:
            await admin.close()
        conn = await asyncpg.connect(dsn)

    try:
        await conn.execute(_schema_sql())
    finally:
        await conn.close()

    print("Schema applied successfully.")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

