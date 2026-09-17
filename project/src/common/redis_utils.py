"""Redis utility clients, key schemas, and deduplication operations."""

from typing import Optional

import redis
import redis.asyncio as aioredis

from src.common.config import get_settings


# Key schema builders matching Section 2.3 of Technical Specification
def dedup_key(event_id: str, prefix: str = "ps:dedup") -> str:
    return f"{prefix}:{event_id}"


def events_today_key() -> str:
    return "ps:count:events_today"


def type_count_key(event_type: str) -> str:
    return f"ps:count:type:{event_type}"


def source_count_key(source: str) -> str:
    return f"ps:count:source:{source}"


def community_count_key(community_id: str) -> str:
    return f"ps:count:community:{community_id}"


def events_per_sec_key() -> str:
    return "ps:rate:events_per_sec"


def active_users_key() -> str:
    return "ps:rate:active_users"


def trending_communities_key() -> str:
    return "ps:trending:communities"


def anomaly_user_key(user_id: str) -> str:
    return f"ps:anomaly:user:{user_id}"


def anomaly_community_key(community_id: str) -> str:
    return f"ps:anomaly:community:{community_id}"


def recent_events_key() -> str:
    return "ps:recent:events"


def recent_anomalies_key() -> str:
    return "ps:anomalies:recent"


def cache_key(endpoint_hash: str) -> str:
    return f"ps:cache:{endpoint_hash}"


# Client factories
def get_redis_client(redis_url: Optional[str] = None) -> redis.Redis:
    """Returns a synchronous Redis client with connection pooling and timeouts."""
    url = redis_url or get_settings().redis_url
    return redis.Redis.from_url(
        url,
        decode_responses=True,
        socket_timeout=1.5,
        socket_connect_timeout=1.5,
    )


def get_async_redis_client(redis_url: Optional[str] = None) -> aioredis.Redis:
    """Returns an asynchronous Redis client with connection pooling and timeouts."""
    url = redis_url or get_settings().redis_url
    return aioredis.from_url(
        url,
        decode_responses=True,
        socket_timeout=1.5,
        socket_connect_timeout=1.5,
    )


# Synchronous deduplication helpers
def is_duplicate_sync(client: redis.Redis, event_id: str) -> bool:
    """Checks if an event_id was previously recorded in Redis."""
    return bool(client.exists(dedup_key(event_id)))


def mark_seen_sync(client: redis.Redis, event_id: str, ttl_seconds: int = 3600) -> None:
    """Marks an event_id as seen with a time-to-live (default 1 hour)."""
    client.setex(dedup_key(event_id), ttl_seconds, "1")


def check_and_set_dedup_sync(
    client: redis.Redis,
    event_id: str,
    ttl_seconds: int = 3600,
) -> bool:
    """Atomically marks an event as seen if not already present.

    Returns True if the event was already seen (duplicate), False if it is new.
    """
    key = dedup_key(event_id)
    # SETNX returns True if key was set (i.e. was NOT already existing)
    was_set = client.set(key, "1", ex=ttl_seconds, nx=True)
    return not was_set


# Asynchronous deduplication helpers
async def is_duplicate_async(client: aioredis.Redis, event_id: str) -> bool:
    """Async check if an event_id was previously recorded in Redis."""
    return bool(await client.exists(dedup_key(event_id)))


async def mark_seen_async(client: aioredis.Redis, event_id: str, ttl_seconds: int = 3600) -> None:
    """Async mark an event_id as seen with a time-to-live (default 1 hour)."""
    await client.setex(dedup_key(event_id), ttl_seconds, "1")


async def check_and_set_dedup_async(
    client: aioredis.Redis,
    event_id: str,
    ttl_seconds: int = 3600,
    prefix: str = "ps:dedup",
) -> bool:
    """Async atomically marks an event as seen if not already present.

    Returns True if the event was already seen (duplicate), False if it is new.
    """
    key = dedup_key(event_id, prefix=prefix)
    was_set = await client.set(key, "1", ex=ttl_seconds, nx=True)
    return not was_set
