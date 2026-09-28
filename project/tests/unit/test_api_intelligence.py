"""Unit tests for Intelligence endpoints."""

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from src.api.main import app


@pytest.mark.asyncio
async def test_get_cross_source_alerts_from_redis():
    mock_redis = MagicMock()
    fake_alert = {
        "topic": "OpenAI",
        "wikipedia_count": 12,
        "hackernews_count": 5,
        "severity": "HIGH",
        "description": "Cross-Source Surge: OpenAI",
        "detected_at": "2026-09-22T10:00:00Z",
        "event_ids": [str(uuid.uuid4())],
    }
    mock_redis.lrange = AsyncMock(return_value=[json.dumps(fake_alert)])
    app.state.redis_client = mock_redis
    app.state.db_pool = None

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/api/v1/intelligence/cross-source?limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["topic"] == "OpenAI"
        assert data[0]["wikipedia_count"] == 12
        assert data[0]["hackernews_count"] == 5


@pytest.mark.asyncio
async def test_get_cross_source_alerts_fallback_db():
    event_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_conn.fetch = AsyncMock(
        return_value=[
            {
                "id": 1,
                "anomaly_type": "CROSS_SOURCE_CORRELATION",
                "severity": "HIGH",
                "description": "Cross-Source Surge on Boeing",
                "details": json.dumps(
                    {
                        "topic": "Boeing",
                        "wikipedia_count": 8,
                        "hackernews_count": 4,
                    }
                ),
                "detected_at": now,
                "event_ids": [event_id],
            }
        ]
    )
    mock_db.acquire.return_value.__aenter__.return_value = mock_conn

    mock_redis = MagicMock()
    mock_redis.lrange = AsyncMock(return_value=[])  # Empty in Redis -> fallback to DB

    app.state.redis_client = mock_redis
    app.state.db_pool = mock_db

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/api/v1/intelligence/cross-source?limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["topic"] == "Boeing"
        assert data[0]["wikipedia_count"] == 8
        assert data[0]["hackernews_count"] == 4


@pytest.mark.asyncio
async def test_get_hot_topics():
    mock_redis = MagicMock()
    mock_redis.zrevrange = AsyncMock(
        return_value=[("OpenAI", 25.0), ("DeepSeek", 18.0), ("Rust", 9.0)]
    )
    app.state.redis_client = mock_redis

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/api/v1/intelligence/hot-topics?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        assert data[0]["rank"] == 1
        assert data[0]["topic"] == "OpenAI"
        assert data[0]["score"] == 25
        assert data[1]["topic"] == "DeepSeek"
        assert data[1]["score"] == 18


@pytest.mark.asyncio
async def test_get_pulse_summary():
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(
        return_value=[
            [("OpenAI", 25.0), ("Rust", 10.0)],  # hot topics
            [
                json.dumps(
                    {
                        "topic": "OpenAI",
                        "wikipedia_count": 12,
                        "hackernews_count": 5,
                        "github_count": 4,
                        "reddit_count": 4,
                        "severity": "HIGH",
                        "description": "Multi-platform surge",
                    }
                )
            ],  # alerts
            15,  # count_5s -> 3.0 eps
            1500,  # total_today
            200,  # WIKIPEDIA
            500,  # SIMULATOR
            150,  # HACKERNEWS
            80,  # GITHUB
            90,  # REDDIT
        ]
    )
    mock_redis.pipeline.return_value = mock_pipe
    app.state.redis_client = mock_redis

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/api/v1/intelligence/pulse-summary")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "active"
        assert data["events_per_second"] == 3.0
        assert data["total_events_today"] == 1500
        assert len(data["top_topics"]) == 2
        assert "Surge Alert" in data["executive_insight"]
        assert data["source_health"]["GITHUB"] == 80
        assert data["source_health"]["REDDIT"] == 90


@pytest.mark.asyncio
async def test_get_topic_dossier():
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    fake_event = {
        "event_id": "ev_abc",
        "source": "GITHUB",
        "event_type": "POST_CREATED",
        "community_id": "openai/whisper",
        "user_id": "committer",
        "title": "Pushed 2 commit(s) to openai/whisper",
        "timestamp": 1727460000000,
    }
    mock_pipe.execute = AsyncMock(
        return_value=[
            4,  # wiki
            5,  # hn
            6,  # github
            3,  # reddit
            2,  # devto
            [json.dumps(fake_event)],  # recent events
            20.0,  # score
        ]
    )
    mock_redis.pipeline.return_value = mock_pipe
    app.state.redis_client = mock_redis

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/api/v1/intelligence/topic/OpenAI")
        assert resp.status_code == 200
        data = resp.json()
        assert data["topic"] == "OpenAI"
        assert data["status"] == "SURGING"
        assert data["total_activity"] == 20
        assert data["sources"]["GITHUB"] == 6
        assert data["sources"]["HACKERNEWS"] == 5
        assert data["sources"]["DEVTO"] == 2
        assert len(data["recent_events"]) == 1
        assert "openai/whisper" in data["communities"]
        assert "High Multi-Platform Momentum" in data["assessment"]

