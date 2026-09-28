"""Unit tests for Metrics endpoints."""

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from src.api.main import app


@pytest.mark.asyncio
async def test_realtime_metrics():
    mock_redis = MagicMock()
    # Mock cache miss
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.setex = AsyncMock()

    mock_pipe = MagicMock()
    # pipeline returns: total_today, active_users, count_5s, 4 types, 5 sources
    mock_pipe.execute = AsyncMock(
        return_value=[
            1250,  # total_today
            340,  # active_users
            150,  # count_5s -> eps = 30.0
            100,
            50,
            30,
            20,  # types
            80,
            120,
            45,
            60,
            35,  # sources (WIKIPEDIA, SIMULATOR, HACKERNEWS, GITHUB, REDDIT)
        ]
    )
    mock_redis.pipeline.return_value = mock_pipe

    app.state.redis_client = mock_redis

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/api/v1/metrics/realtime")
        assert resp.status_code == 200
        data = resp.json()
        assert data["events_per_second"] == 30.0
        assert data["total_events_today"] == 1250
        assert data["active_users"] == 340
        assert "POST_CREATED" in data["events_by_type"]
        assert "WIKIPEDIA" in data["events_by_source"]
        assert "HACKERNEWS" in data["events_by_source"]
        assert "GITHUB" in data["events_by_source"]
        assert "REDDIT" in data["events_by_source"]


@pytest.mark.asyncio
async def test_trending_communities():
    mock_redis = MagicMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.setex = AsyncMock()
    mock_redis.zrevrange = AsyncMock(
        return_value=[
            ("en.wikipedia.org", 150.0),
            ("tech_news", 85.0),
        ]
    )

    app.state.redis_client = mock_redis

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/api/v1/metrics/trending?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["rank"] == 1
        assert data[0]["community_id"] == "en.wikipedia.org"
        assert data[0]["score"] == 150
