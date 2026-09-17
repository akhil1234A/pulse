"""Unit tests for the Wikipedia SSE connector."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.models import EventSource, EventType
from src.ingestion.connectors.wikipedia import WikipediaConnector


def test_wikipedia_event_mapping_and_normalization():
    connector = WikipediaConnector()

    raw_edit = {
        "namespace": 0,
        "type": "edit",
        "user": "EditorAlpha",
        "wiki": "enwiki",
        "timestamp": 1700000000,
        "title": "Quantum Computing",
        "comment": "Added citations",
        "bot": False,
        "minor": False,
        "server_url": "https://en.wikipedia.org",
        "length": {"old": 1200, "new": 1450},
    }

    normalized = connector.normalize_raw_event(raw_edit)
    assert normalized is not None
    assert normalized["event_type"] == EventType.POST_CREATED.value
    assert normalized["source"] == EventSource.WIKIPEDIA.value
    assert normalized["user_id"] == "EditorAlpha"
    assert normalized["community_id"] == "enwiki"
    assert normalized["timestamp"] == 1700000000 * 1000

    payload = json.loads(normalized["payload"])
    assert payload["title"] == "Quantum Computing"
    assert payload["comment"] == "Added citations"


def test_wikipedia_namespace_filtering():
    connector = WikipediaConnector(allowed_namespaces={0, 2})

    # Namespace 0 (Main article) -> allowed
    assert connector.normalize_raw_event({"namespace": 0, "type": "edit"}) is not None

    # Namespace 2 (User page) -> allowed
    assert connector.normalize_raw_event({"namespace": 2, "type": "edit"}) is not None

    # Namespace 1 (Talk page) -> filtered out
    assert connector.normalize_raw_event({"namespace": 1, "type": "edit"}) is None

    # Namespace 4 (Project/Wikipedia) -> filtered out
    assert connector.normalize_raw_event({"namespace": 4, "type": "edit"}) is None


def test_wikipedia_action_types():
    connector = WikipediaConnector()

    assert (
        connector.normalize_raw_event({"namespace": 0, "type": "new"})["event_type"]
        == EventType.POST_CREATED.value
    )
    assert (
        connector.normalize_raw_event({"namespace": 0, "type": "log"})["event_type"]
        == EventType.USER_JOINED.value
    )
    assert (
        connector.normalize_raw_event({"namespace": 0, "type": "categorize"})["event_type"]
        == EventType.REACTION_ADDED.value
    )
    assert connector.normalize_raw_event({"namespace": 0, "type": "unknown_action"}) is None


@pytest.mark.asyncio
async def test_wikipedia_stream_mock():
    connector = WikipediaConnector()

    sse_data = {
        "namespace": 0,
        "type": "edit",
        "user": "StreamBot",
        "wiki": "dewiki",
        "timestamp": 1700000050,
        "title": "Künstliche Intelligenz",
    }
    raw_sse_lines = [
        b":heartbeat\n",
        b"event: message\n",
        f"data: {json.dumps(sse_data)}\n".encode("utf-8"),
        b"\n",
    ]

    mock_response = AsyncMock()
    mock_response.status = 200

    async def line_generator():
        for line in raw_sse_lines:
            yield line

    mock_response.content = line_generator()

    mock_get = MagicMock()
    mock_get.__aenter__ = AsyncMock(return_value=mock_response)
    mock_get.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.get.return_value = mock_get
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        events = []
        async for item in connector.stream_events():
            events.append(item)
            connector.stop()
            break

        assert len(events) == 1
        assert events[0]["user_id"] == "StreamBot"
        assert events[0]["community_id"] == "dewiki"
