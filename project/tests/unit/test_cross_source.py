"""Unit tests for cross-source intelligence detection and topic extraction."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.common.models import EventSource, EventType, PulseStreamEvent
from src.workers.intelligence.cross_source import (
    CrossSourceIntelligenceDetector,
    extract_candidate_topics,
    normalize_topic,
)


def test_normalize_topic():
    assert normalize_topic("OpenAI (organization)") == "OpenAI"
    assert normalize_topic("Boeing 737 MAX") == "Boeing 737 MAX"
    assert normalize_topic("  Multiple   spaces  ") == "Multiple spaces"


def test_extract_candidate_topics_wikipedia():
    event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.WIKIPEDIA,
        user_id="editor_1",
        payload={"title": "DeepSeek-V3", "comment": "Updated model info"},
    )
    topics = extract_candidate_topics(event)
    assert len(topics) >= 1
    assert "DeepSeek-V3" in topics or "DeepSeek V3" in topics or "DeepSeek" in topics[0]


def test_extract_candidate_topics_hackernews():
    event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.HACKERNEWS,
        user_id="author_1",
        payload={"title": "OpenAI announces new reasoning benchmark with DeepSeek"},
    )
    topics = extract_candidate_topics(event)
    assert any("OpenAI" in t for t in topics)
    assert any("DeepSeek" in t for t in topics)


@pytest.mark.asyncio
async def test_cross_source_surge_triggers_alert():
    detector = CrossSourceIntelligenceDetector(
        wiki_threshold=3,
        hn_threshold=2,
        window_seconds=1800,
        alert_cooldown_seconds=1800,
    )

    wiki_event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.WIKIPEDIA,
        user_id="editor_1",
        payload={"title": "DeepSeek"},
    )

    # Mock Redis client
    mock_redis = MagicMock()
    mock_redis.incr = AsyncMock(return_value=3)
    mock_redis.expire = AsyncMock()
    mock_redis.zadd = AsyncMock()

    mock_pipe = MagicMock()
    mock_pipe.get = MagicMock()
    # counts: wiki=3, hn=2
    mock_pipe.execute = AsyncMock(return_value=["3", "2"])
    mock_pipe.lpush = MagicMock()
    mock_pipe.ltrim = MagicMock()
    mock_pipe.publish = MagicMock()

    mock_redis.pipeline.return_value = mock_pipe
    # Cooldown check: returns True (set was successful, new alert)
    mock_redis.set = AsyncMock(return_value=True)

    alerts = await detector.evaluate_event(wiki_event, redis_client=mock_redis)

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.topic == "DeepSeek"
    assert alert.wikipedia_count == 3
    assert alert.hackernews_count == 2
    assert alert.severity == "HIGH"
    assert "DeepSeek" in alert.description
    assert alert.event_ids == [wiki_event.event_id]


@pytest.mark.asyncio
async def test_cross_source_no_alert_when_threshold_not_met():
    detector = CrossSourceIntelligenceDetector(
        wiki_threshold=5,
        hn_threshold=3,
    )

    event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.WIKIPEDIA,
        user_id="editor_1",
        payload={"title": "Obscure Article"},
    )

    mock_redis = MagicMock()
    mock_redis.incr = AsyncMock(return_value=1)
    mock_redis.expire = AsyncMock()
    mock_redis.zadd = AsyncMock()

    mock_pipe = MagicMock()
    mock_pipe.get = MagicMock()
    # counts: wiki=1, hn=0
    mock_pipe.execute = AsyncMock(return_value=["1", "0"])
    mock_redis.pipeline.return_value = mock_pipe

    alerts = await detector.evaluate_event(event, redis_client=mock_redis)
    assert len(alerts) == 0
