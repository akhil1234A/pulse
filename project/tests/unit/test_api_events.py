"""Unit tests for Events endpoints."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import uuid
import httpx
import pytest
from src.api.main import app


@pytest.mark.asyncio
async def test_list_events():
    event_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_conn.fetchval = AsyncMock(return_value=1)
    mock_conn.fetch = AsyncMock(return_value=[{
        "event_id": event_id,
        "event_type": "POST_CREATED",
        "source": "WIKIPEDIA",
        "timestamp": now,
        "user_id": "test_user",
        "community_id": "test_comm",
        "payload": '{"title": "Test Page"}',
        "ingested_at": now,
        "processed_at": now,
        "schema_version": "1.0",
    }])
    mock_db.acquire.return_value.__aenter__.return_value = mock_conn

    app.state.db_pool = mock_db

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/v1/events?page=1&page_size=10&community_id=test_comm")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["event_id"] == str(event_id)
        assert data["items"][0]["payload"] == {"title": "Test Page"}


@pytest.mark.asyncio
async def test_get_event_by_id():
    event_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_conn.fetchrow = AsyncMock(return_value={
        "event_id": event_id,
        "event_type": "USER_JOINED",
        "source": "SIMULATOR",
        "timestamp": now,
        "user_id": "user_123",
        "community_id": "gaming",
        "payload": "{}",
        "ingested_at": now,
        "processed_at": now,
        "schema_version": "1.0",
    })
    mock_db.acquire.return_value.__aenter__.return_value = mock_conn

    app.state.db_pool = mock_db

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # Valid ID
        resp = await client.get(f"/api/v1/events/{event_id}")
        assert resp.status_code == 200
        assert resp.json()["event_id"] == str(event_id)

        # Invalid UUID format -> 404
        resp404 = await client.get("/api/v1/events/invalid-uuid-format")
        assert resp404.status_code == 404
