"""Unit tests for Dev.to Community Articles connector."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.models import EventSource, EventType
from src.common.schemas.avro import deserialize_event, serialize_event
from src.ingestion.connectors.devto import DevtoConnector


def test_devto_article_normalization():
    connector = DevtoConnector()

    raw_article = {
        "id": 1982734,
        "title": "Building a Realtime Multi-Agent System with Python",
        "description": "A comprehensive deep dive into asynchronous streaming pipelines.",
        "published_at": "2026-09-28T10:30:00Z",
        "url": "https://dev.to/pythondriver/building-realtime-agents",
        "tag_list": ["python", "ai", "architecture"],
        "public_reactions_count": 88,
        "comments_count": 14,
        "reading_time_minutes": 6,
        "user": {
            "name": "Jane Pythonista",
            "username": "pythondriver",
        },
    }

    normalized = connector.normalize_raw_event(raw_article)
    assert normalized is not None
    assert normalized["event_type"] == EventType.POST_CREATED.value
    assert normalized["source"] == EventSource.DEVTO.value
    assert normalized["user_id"] == "pythondriver"
    assert normalized["community_id"] == "devto/python"
    assert normalized["schema_version"] == "1.0"

    payload = json.loads(normalized["payload"])
    assert payload["title"] == "Building a Realtime Multi-Agent System with Python"
    assert payload["reactions"] == 88
    assert payload["comments_count"] == 14
    assert payload["reading_time_minutes"] == 6
    assert payload["author_name"] == "Jane Pythonista"
    assert payload["devto_id"] == 1982734

    # Verify Avro serialization and deserialization
    avro_bytes = serialize_event(normalized)
    assert isinstance(avro_bytes, bytes)
    deserialized = deserialize_event(avro_bytes)
    assert deserialized["event_id"] == normalized["event_id"]
    assert deserialized["source"] == "DEVTO"


def test_devto_empty_or_invalid_article_skipped():
    connector = DevtoConnector()

    # Missing ID
    assert connector.normalize_raw_event({"title": "Valid title", "id": None}) is None

    # Empty title
    assert connector.normalize_raw_event({"title": "", "id": 12345}) is None

    # Whitespace title
    assert connector.normalize_raw_event({"title": "   ", "id": 12345}) is None


def test_devto_deduplication():
    connector = DevtoConnector(max_seen_ids=5)
    connector._mark_seen(101)
    assert connector._is_seen(101)

    # Re-marking does not duplicate
    connector._mark_seen(101)
    assert len(connector._seen_ids) == 1

    # Overfill capacity
    for i in range(102, 108):
        connector._mark_seen(i)

    assert len(connector._seen_ids) == 5
    assert not connector._is_seen(101)  # Evicted
    assert connector._is_seen(107)


@pytest.mark.asyncio
async def test_devto_stream_events():
    connector = DevtoConnector(poll_interval=0.01, initial_backfill_count=2)

    fake_articles = [
        {
            "id": 1001,
            "title": "Rust vs Go for Microservices in 2026",
            "published_at": "2026-09-28T09:00:00Z",
            "tag_list": ["rust", "golang"],
            "public_reactions_count": 42,
            "user": {"name": "Systems Guy", "username": "systemsguy"},
        },
        {
            "id": 1002,
            "title": "Optimizing Kafka Ingestion with Zero-Copy Buffers",
            "published_at": "2026-09-28T09:15:00Z",
            "tag_list": ["kafka", "data-engineering"],
            "public_reactions_count": 95,
            "user": {"name": "Data Master", "username": "datamaster"},
        },
    ]

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.json = AsyncMock(return_value=fake_articles)
    mock_resp.text = AsyncMock(return_value=json.dumps(fake_articles))
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        events = []
        async for ev in connector.stream_events():
            events.append(ev)
            if len(events) >= 2:
                connector.stop()
                break

        assert len(events) == 2
        assert events[0]["community_id"] == "devto/kafka"
        assert events[1]["community_id"] == "devto/rust"
        assert events[0]["source"] == "DEVTO"
        assert events[1]["source"] == "DEVTO"
