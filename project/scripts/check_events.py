import asyncio
import asyncpg
from src.common.config import get_settings

async def main():
    s = get_settings()
    conn = await asyncpg.connect(s.postgres_dsn)
    c = await conn.fetchval("SELECT count(event_id) FROM events WHERE user_id = 'debug_user'")
    print("Found debug_user rows:", c)
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
