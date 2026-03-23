import asyncio
import asyncpg
import sys

async def main():
    conn = await asyncpg.connect("postgresql://postgres:abdu@localhost:5432/abdu")
    rows = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = [r['table_name'] for r in rows]
    print(f"Tables in DB: {tables}")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
