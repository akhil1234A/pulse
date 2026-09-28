"""Unit tests for Reddit JSON public feed connector."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.models import EventSource, EventType
from src.ingestion.connectors.reddit import RedditConnector


def test_reddit_post_normalization():
    connector = RedditConnector()

    raw_post = {
        "data": {
            "id": "t3_12345",
            "title": "OpenAI announces next-generation reasoning model",
            "author": "tech_enthusiast",
            "subreddit": "technology",
            "score": 1500,
            "num_comments": 420,
            "url": "https://openai.com/blog/reasoning",
            "permalink": "/r/technology/comments/12345/openai_announces/",
            "created_utc": 1727460000,
            "link_flair_text": "AI",
            "selftext": "Discussion on the latest announcement...",
        }
    }

    normalized = connector.normalize_raw_event(raw_post)
    assert normalized is not None
    assert normalized["event_type"] == EventType.POST_CREATED.value
    assert normalized["source"] == EventSource.REDDIT.value
    assert normalized["user_id"] == "tech_enthusiast"
    assert normalized["community_id"] == "r/technology"
    assert normalized["timestamp"] == 1727460000 * 1000

    payload = json.loads(normalized["payload"])
    assert payload["title"] == "OpenAI announces next-generation reasoning model"
    assert payload["score"] == 1500
    assert payload["num_comments"] == 420
    assert payload["reddit_id"] == "t3_12345"
    assert payload["url"] == "https://reddit.com/r/technology/comments/12345/openai_announces/"


def test_reddit_empty_title_skipped():
    connector = RedditConnector()
    raw_post = {
        "data": {
            "id": "t3_9999",
            "title": "",
            "author": "someone",
            "subreddit": "programming",
        }
    }
    assert connector.normalize_raw_event(raw_post) is None


def test_reddit_deduplication():
    connector = RedditConnector(max_seen_ids=5)
    connector._mark_seen("p1")
    assert "p1" in connector._seen_ids

    for i in range(2, 8):
        connector._mark_seen(f"p{i}")

    assert len(connector._seen_ids) == 5
    assert "p1" not in connector._seen_ids


@pytest.mark.asyncio
async def test_reddit_stream_events():
    connector = RedditConnector(poll_interval=0.01, initial_backfill_count=2)

    fake_children = [
        {
            "data": {
                "id": "r_1",
                "title": "Python 3.14 alpha released with JIT",
                "author": "guido_fan",
                "subreddit": "programming",
                "score": 300,
                "created_utc": 1727460100,
            }
        },
        {
            "data": {
                "id": "r_2",
                "title": "Discussion on open source AI agents",
                "author": "agent_smith",
                "subreddit": "artificial",
                "score": 85,
                "created_utc": 1727460200,
            }
        },
    ]

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.json = AsyncMock(return_value={"data": {"children": fake_children}})
    mock_resp.text = AsyncMock(return_value=json.dumps({"data": {"children": fake_children}}))
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
        assert events[0]["community_id"] == "r/programming"
        assert events[1]["community_id"] == "r/artificial"
        assert events[0]["source"] == "REDDIT"


def test_reddit_atom_rss_parsing():
    from src.ingestion.connectors.reddit import _parse_atom_xml

    sample_atom = """<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <entry>
        <title>New AI Agent Framework in Rust</title>
        <id>t3_test123</id>
        <link href="https://www.reddit.com/r/rust/comments/test123/new_ai_agent/"/>
        <author><name>/u/rustacean</name></author>
        <published>2026-09-28T10:00:00+00:00</published>
        <category term="r/rust" label="r/rust"/>
      </entry>
    </feed>
    """
    posts = _parse_atom_xml(sample_atom)
    assert len(posts) == 1
    assert posts[0]["id"] == "t3_test123"
    assert posts[0]["title"] == "New AI Agent Framework in Rust"
    assert posts[0]["author"] == "rustacean"
    assert posts[0]["subreddit"] == "rust"
    assert posts[0]["url"] == "https://www.reddit.com/r/rust/comments/test123/new_ai_agent/"

