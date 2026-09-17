"""Unit tests for Redis utilities and key schemes."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.common.redis_utils import (
    active_users_key,
    anomaly_community_key,
    anomaly_user_key,
    cache_key,
    check_and_set_dedup_async,
    check_and_set_dedup_sync,
    community_count_key,
    dedup_key,
    events_per_sec_key,
    events_today_key,
    is_duplicate_async,
    is_duplicate_sync,
    mark_seen_async,
    mark_seen_sync,
    source_count_key,
    trending_communities_key,
    type_count_key,
)


def test_key_schemas():
    assert dedup_key("evt_1") == "ps:dedup:evt_1"
    assert events_today_key() == "ps:count:events_today"
    assert type_count_key("POST_CREATED") == "ps:count:type:POST_CREATED"
    assert source_count_key("WIKIPEDIA") == "ps:count:source:WIKIPEDIA"
    assert community_count_key("python") == "ps:count:community:python"
    assert events_per_sec_key() == "ps:rate:events_per_sec"
    assert active_users_key() == "ps:rate:active_users"
    assert trending_communities_key() == "ps:trending:communities"
    assert anomaly_user_key("user_1") == "ps:anomaly:user:user_1"
    assert anomaly_community_key("comm_1") == "ps:anomaly:community:comm_1"
    assert cache_key("hash123") == "ps:cache:hash123"


def test_sync_dedup_helpers():
    mock_redis = MagicMock()

    # is_duplicate
    mock_redis.exists.return_value = 1
    assert is_duplicate_sync(mock_redis, "evt_1") is True
    mock_redis.exists.return_value = 0
    assert is_duplicate_sync(mock_redis, "evt_2") is False

    # mark_seen
    mark_seen_sync(mock_redis, "evt_3", 1800)
    mock_redis.setex.assert_called_with("ps:dedup:evt_3", 1800, "1")

    # check_and_set_dedup: when set succeeds (key was new), was_set=True -> return False
    mock_redis.set.return_value = True
    assert check_and_set_dedup_sync(mock_redis, "evt_4") is False

    # when set returns None/False (key was already present), was_set=False -> return True (is duplicate)
    mock_redis.set.return_value = None
    assert check_and_set_dedup_sync(mock_redis, "evt_5") is True


@pytest.mark.asyncio
async def test_async_dedup_helpers():
    mock_redis = AsyncMock()

    # is_duplicate_async
    mock_redis.exists.return_value = 1
    assert await is_duplicate_async(mock_redis, "evt_async_1") is True

    # mark_seen_async
    await mark_seen_async(mock_redis, "evt_async_2", 3600)
    mock_redis.setex.assert_called_with("ps:dedup:evt_async_2", 3600, "1")

    # check_and_set_dedup_async
    mock_redis.set.return_value = True
    assert await check_and_set_dedup_async(mock_redis, "evt_async_3") is False

    mock_redis.set.return_value = False
    assert await check_and_set_dedup_async(mock_redis, "evt_async_4") is True
