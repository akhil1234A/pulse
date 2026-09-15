"""PostgreSQL asynchronous connection pool and batch insert helpers using asyncpg."""

import json
import logging
from typing import Any, Dict, List, Optional
import asyncpg
from src.common.config import get_settings
from src.common.models import PulseStreamEvent

logger = logging.getLogger(__name__)

EVENT_COLUMNS = [
    "event_id",
    "event_type",
    "source",
    "timestamp",
    "user_id",
    "community_id",
    "payload",
    "ingested_at",
    "processed_at",
    "schema_version",
]


async def create_db_pool(
    dsn: Optional[str] = None,
    min_size: int = 2,
    max_size: int = 10,
) -> asyncpg.Pool:
    """Creates an asyncpg connection pool."""
    database_dsn = dsn or get_settings().postgres_dsn
    return await asyncpg.create_pool(
        dsn=database_dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=60,
    )


async def close_db_pool(pool: Optional[asyncpg.Pool]) -> None:
    """Closes all connections in the pool gracefully."""
    if pool is not None:
        await pool.close()


async def batch_insert_events(
    pool: asyncpg.Pool,
    events: List[PulseStreamEvent],
) -> int:
    """Inserts a batch of events into the partitioned events table using PostgreSQL COPY protocol.

    Returns the number of rows inserted.
    """
    if not events:
        return 0

    records = [event.to_db_record() for event in events]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.copy_records_to_table(
                "events",
                records=records,
                columns=EVENT_COLUMNS,
            )
    logger.debug("Successfully inserted batch of %d events via COPY", len(records))
    return len(records)


async def insert_dead_letter_event(
    pool: asyncpg.Pool,
    raw_event: Any,
    error_message: str,
    error_type: str,
    retry_count: int = 0,
) -> int:
    """Records an unparseable or rejected event into the dead_letter_events table."""
    payload_json = (
        raw_event
        if isinstance(raw_event, str)
        else json.dumps(raw_event, default=str)
    )
    query = """
        INSERT INTO dead_letter_events (raw_event, error_message, error_type, retry_count)
        VALUES ($1::jsonb, $2, $3, $4)
        RETURNING id;
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, payload_json, error_message, error_type, retry_count)
        return row["id"]
