"""Unit tests for Anomaly Detection Rules."""

from unittest.mock import AsyncMock, MagicMock
import pytest

from src.common.models import EventSource, EventType, PulseStreamEvent
from src.workers.anomaly.rules import CommunitySpikeRule, DuplicateStormRule, UserFloodRule


@pytest.fixture
def sample_event():
    return PulseStreamEvent(
        event_id="test-evt-1",
        event_type=EventType.POST_CREATED,
        source=EventSource.SIMULATOR,
        user_id="user_spammer",
        community_id="community_hot",
    )


# --- UserFloodRule Tests ---
@pytest.mark.asyncio
async def test_user_flood_rule_no_trigger(sample_event):
    rule = UserFloodRule(threshold=50, window_seconds=60)
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    # Mock pipeline returns: [zadd_res, zrem_res, zcard_res (e.g. 50), expire_res]
    mock_pipe.execute = AsyncMock(return_value=[1, 0, 50, True])
    mock_redis.pipeline.return_value.__aenter__.return_value = mock_pipe

    anomaly = await rule.evaluate_event(sample_event, mock_redis)
    assert anomaly is None


@pytest.mark.asyncio
async def test_user_flood_rule_trigger(sample_event):
    rule = UserFloodRule(threshold=50, window_seconds=60)
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    # 51 events in window
    mock_pipe.execute = AsyncMock(return_value=[1, 0, 51, True])
    mock_redis.pipeline.return_value.__aenter__.return_value = mock_pipe

    anomaly = await rule.evaluate_event(sample_event, mock_redis)
    assert anomaly is not None
    assert anomaly.anomaly_type == "USER_FLOOD"
    assert anomaly.severity == "HIGH"
    assert anomaly.details["count"] == 51
    assert "user_spammer" in anomaly.description


@pytest.mark.asyncio
async def test_user_flood_rule_missing_user():
    rule = UserFloodRule(threshold=50)
    event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.SIMULATOR,
        user_id="",
    )
    anomaly = await rule.evaluate_event(event, MagicMock())
    assert anomaly is None


# --- CommunitySpikeRule Tests ---
@pytest.mark.asyncio
async def test_community_spike_rule_no_trigger(sample_event):
    rule = CommunitySpikeRule(multiplier_threshold=10.0, min_events=10)
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    # results: [zadd, zrem, count_1min (15), count_1hour (180), expire]
    # avg_per_min = 180 / 60 = 3. 10x avg = 30. count_1min is 15 <= 30 -> no trigger
    mock_pipe.execute = AsyncMock(return_value=[1, 0, 15, 180, True])
    mock_redis.pipeline.return_value.__aenter__.return_value = mock_pipe

    anomaly = await rule.evaluate_event(sample_event, mock_redis)
    assert anomaly is None


@pytest.mark.asyncio
async def test_community_spike_rule_trigger(sample_event):
    rule = CommunitySpikeRule(multiplier_threshold=10.0, min_events=10)
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    # results: [zadd, zrem, count_1min (50), count_1hour (120), expire]
    # avg_per_min = 120 / 60 = 2. 10x avg = 20. count_1min is 50 > 20 -> trigger!
    mock_pipe.execute = AsyncMock(return_value=[1, 0, 50, 120, True])
    mock_redis.pipeline.return_value.__aenter__.return_value = mock_pipe

    anomaly = await rule.evaluate_event(sample_event, mock_redis)
    assert anomaly is not None
    assert anomaly.anomaly_type == "COMMUNITY_SPIKE"
    assert anomaly.severity == "MEDIUM"
    assert "community_hot" in anomaly.description


@pytest.mark.asyncio
async def test_community_spike_rule_no_community():
    rule = CommunitySpikeRule()
    event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.SIMULATOR,
        user_id="u1",
        community_id=None,
    )
    anomaly = await rule.evaluate_event(event, MagicMock())
    assert anomaly is None


# --- DuplicateStormRule Tests ---
def test_duplicate_storm_rule_no_trigger():
    rule = DuplicateStormRule(threshold_ratio=0.05, min_batch_size=10)
    # 4 duplicates out of 100 = 4% <= 5%
    anomaly = rule.evaluate_batch(
        duplicate_count=4,
        total_count=100,
        duplicate_event_ids=["d1", "d2", "d3", "d4"],
    )
    assert anomaly is None


def test_duplicate_storm_rule_trigger():
    rule = DuplicateStormRule(threshold_ratio=0.05, min_batch_size=10)
    # 10 duplicates out of 100 = 10% > 5%
    dup_ids = [f"d{i}" for i in range(10)]
    anomaly = rule.evaluate_batch(
        duplicate_count=10,
        total_count=100,
        duplicate_event_ids=dup_ids,
    )
    assert anomaly is not None
    assert anomaly.anomaly_type == "DUPLICATE_STORM"
    assert anomaly.severity == "HIGH"
    assert anomaly.details["duplicate_ratio"] == 0.10


def test_duplicate_storm_rule_small_batch():
    rule = DuplicateStormRule(threshold_ratio=0.05, min_batch_size=10)
    # Below min_batch_size of 10
    anomaly = rule.evaluate_batch(
        duplicate_count=2,
        total_count=5,
        duplicate_event_ids=["d1", "d2"],
    )
    assert anomaly is None
