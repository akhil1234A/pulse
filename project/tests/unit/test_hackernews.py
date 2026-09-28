"""Unit tests for the Hacker News REST API connector."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.models import EventSource, EventType
from src.ingestion.connectors.hackernews import HackerNewsConnector


def test_hackernews_story_normalization():
    connector = HackerNewsConnector()

    raw_story = {
        "id": 12345,
        "by": "pg",
        "type": "story",
        "time": 1700000000,
        "title": "Show HN: PulseStream Live Engine",
        "url": "https://github.com/pulsestream/pulse",
        "score": 42,
        "descendants": 10,
    }

    normalized = connector.normalize_raw_event(raw_story)
    assert normalized is not None
    assert normalized["event_type"] == EventType.POST_CREATED.value
    assert normalized["source"] == EventSource.HACKERNEWS.value
    assert normalized["user_id"] == "pg"
    assert normalized["community_id"] == "github.com"
    assert normalized["timestamp"] == 1700000000 * 1000

    payload = json.loads(normalized["payload"])
    assert payload["title"] == "Show HN: PulseStream Live Engine"
    assert payload["score"] == 42
    assert payload["hn_id"] == 12345
    assert payload["hn_type"] == "story"


def test_hackernews_ask_hn_community():
    connector = HackerNewsConnector()

    raw_ask = {
        "id": 99999,
        "by": "techguy",
        "type": "story",
        "time": 1700000100,
        "title": "Ask HN: What is your streaming architecture?",
        "score": 15,
    }

    normalized = connector.normalize_raw_event(raw_ask)
    assert normalized is not None
    assert normalized["community_id"] == "self.hackernews"


def test_hackernews_comment_normalization():
    connector = HackerNewsConnector()

    raw_comment = {
        "id": 55555,
        "by": "commenter1",
        "type": "comment",
        "time": 1700000200,
        "text": "This looks like a solid project using Kafka and Flink/Redis!",
    }

    normalized = connector.normalize_raw_event(raw_comment)
    assert normalized is not None
    assert normalized["event_type"] == EventType.COMMENT_CREATED.value
    assert normalized["community_id"] == "self.hackernews"


def test_hackernews_deleted_or_dead_filtered():
    connector = HackerNewsConnector()
    assert connector.normalize_raw_event({"id": 1, "deleted": True}) is None
    assert connector.normalize_raw_event({"id": 2, "dead": True}) is None
    assert connector.normalize_raw_event({}) is None


@pytest.mark.asyncio
async def test_hackernews_stream_events():
    connector = HackerNewsConnector(poll_interval=0.01, initial_backfill_count=2)

    fake_item_1 = {
        "id": 101,
        "by": "dev1",
        "type": "story",
        "time": 1700000000,
        "title": "Article 1",
        "url": "https://news.ycombinator.com",
    }
    fake_item_2 = {
        "id": 102,
        "by": "dev2",
        "type": "story",
        "time": 1700000010,
        "title": "Article 2",
        "url": "https://example.com/test",
    }

    def mock_get(url, *args, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status = 200
        if "newstories.json" in url:
            mock_resp.json = AsyncMock(return_value=[101, 102])
        elif "item/101.json" in url:
            mock_resp.json = AsyncMock(return_value=fake_item_1)
        elif "item/102.json" in url:
            mock_resp.json = AsyncMock(return_value=fake_item_2)
        else:
            mock_resp.json = AsyncMock(return_value=None)

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=mock_resp)
        cm.__aexit__ = AsyncMock(return_value=None)
        return cm

    mock_session = MagicMock()
    mock_session.get = mock_get
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        events = []
        async for event in connector.stream_events():
            events.append(event)
            if len(events) >= 2:
                connector.stop()
                break

        assert len(events) == 2
        titles = [json.loads(e["payload"])["title"] for e in events]
        assert "Article 1" in titles
        assert "Article 2" in titles
