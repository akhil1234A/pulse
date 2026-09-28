"""Unit tests for GitHub Public Events connector."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.models import EventSource, EventType
from src.ingestion.connectors.github_events import GitHubEventsConnector


def test_github_event_normalization_watch():
    connector = GitHubEventsConnector()

    raw_event = {
        "id": "123456789",
        "type": "WatchEvent",
        "actor": {"login": "octocat"},
        "repo": {"name": "torvalds/linux"},
        "payload": {"action": "started"},
        "created_at": "2026-09-27T12:00:00Z",
    }

    normalized = connector.normalize_raw_event(raw_event)
    assert normalized is not None
    assert normalized["event_type"] == EventType.REACTION_ADDED.value
    assert normalized["source"] == EventSource.GITHUB.value
    assert normalized["user_id"] == "octocat"
    assert normalized["community_id"] == "torvalds/linux"

    payload = json.loads(normalized["payload"])
    assert "Starred torvalds/linux" in payload["title"]
    assert payload["repo"] == "torvalds/linux"
    assert payload["gh_type"] == "WatchEvent"


def test_github_event_normalization_push():
    connector = GitHubEventsConnector()

    raw_event = {
        "id": "987654321",
        "type": "PushEvent",
        "actor": {"login": "dev_user"},
        "repo": {"name": "facebook/react"},
        "payload": {
            "ref": "refs/heads/main",
            "commits": [{"sha": "abc1234"}],
        },
        "created_at": "2026-09-27T12:05:00Z",
    }

    normalized = connector.normalize_raw_event(raw_event)
    assert normalized is not None
    assert normalized["event_type"] == EventType.POST_CREATED.value
    assert normalized["source"] == EventSource.GITHUB.value
    assert normalized["community_id"] == "facebook/react"

    payload = json.loads(normalized["payload"])
    assert "Pushed 1 commit(s) to facebook/react on main" in payload["title"]


def test_github_event_deduplication():
    connector = GitHubEventsConnector(max_seen_ids=10)
    connector._mark_seen("ev_1")
    assert "ev_1" in connector._seen_ids

    # Marking same id again does not duplicate
    connector._mark_seen("ev_1")
    assert len(connector._seen_ids) == 1

    # Fill beyond capacity
    for i in range(2, 15):
        connector._mark_seen(f"ev_{i}")

    assert len(connector._seen_ids) == 10
    assert "ev_1" not in connector._seen_ids  # ev_1 was evicted


@pytest.mark.asyncio
async def test_github_stream_events_yields_new_items():
    connector = GitHubEventsConnector(poll_interval=0.01, initial_backfill_count=2)

    fake_events = [
        {
            "id": "gh_1",
            "type": "ForkEvent",
            "actor": {"login": "forker"},
            "repo": {"name": "pallets/flask"},
            "payload": {"forkee": {"full_name": "forker/flask"}},
            "created_at": "2026-09-27T12:00:00Z",
        },
        {
            "id": "gh_2",
            "type": "IssuesEvent",
            "actor": {"login": "issue_opener"},
            "repo": {"name": "pallets/flask"},
            "payload": {"action": "opened", "issue": {"title": "Bug in routing"}},
            "created_at": "2026-09-27T12:01:00Z",
        },
    ]

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=fake_events)
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

        assert len(events) == 2
        assert events[0]["community_id"] == "pallets/flask"
        assert events[0]["source"] == "GITHUB"
