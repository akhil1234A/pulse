"""Unit tests for Health Check endpoint."""

from unittest.mock import AsyncMock, MagicMock, patch
import httpx
import pytest
from src.api.main import app


@pytest.mark.asyncio
async def test_health_all_ok():
    mock_redis = MagicMock()
    mock_redis.ping = AsyncMock(return_value=True)

    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_conn.fetchval = AsyncMock(return_value=1)
    mock_db.acquire.return_value.__aenter__.return_value = mock_conn

    app.state.redis_client = mock_redis
    app.state.db_pool = mock_db

    with patch("confluent_kafka.admin.AdminClient.list_topics", return_value=MagicMock(topics={"pulsestream.events": None})):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/api/v1/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "healthy"
            assert data["components"]["redis"] == "ok"
            assert data["components"]["postgres"] == "ok"
            assert data["components"]["kafka"] == "ok"


@pytest.mark.asyncio
async def test_health_degraded_when_redis_down():
    mock_redis = MagicMock()
    mock_redis.ping = AsyncMock(side_effect=Exception("Redis timeout"))

    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_conn.fetchval = AsyncMock(return_value=1)
    mock_db.acquire.return_value.__aenter__.return_value = mock_conn

    app.state.redis_client = mock_redis
    app.state.db_pool = mock_db

    with patch("confluent_kafka.admin.AdminClient.list_topics", return_value=MagicMock(topics={})):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/api/v1/health")
            assert resp.status_code == 503
            data = resp.json()
            assert data["status"] == "degraded"
            assert data["components"]["redis"] == "down"
            assert data["components"]["postgres"] == "ok"
