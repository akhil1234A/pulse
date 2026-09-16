"""Unit tests for Anomalies endpoints."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import uuid
import httpx
import pytest
from src.api.main import app


@pytest.mark.asyncio
async def test_list_anomalies():
    event_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_conn.fetch = AsyncMock(return_value=[{
        "id": 1,
        "anomaly_type": "USER_FLOOD",
        "severity": "CRITICAL",
        "description": "User spamming events",
        "details": '{"count": 65}',
        "detected_at": now,
        "event_ids": [event_id],
    }])
    mock_db.acquire.return_value.__aenter__.return_value = mock_conn

    app.state.db_pool = mock_db

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/v1/anomalies?limit=10&severity=CRITICAL")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["anomaly_type"] == "USER_FLOOD"
        assert data[0]["severity"] == "CRITICAL"
        assert data[0]["details"] == {"count": 65}
        assert data[0]["event_ids"] == [str(event_id)]
