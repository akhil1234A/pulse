"""Unit tests for WebSocket ConnectionManager."""

from unittest.mock import AsyncMock, MagicMock
import pytest
from src.api.websocket import ConnectionManager, build_dashboard_payload


@pytest.mark.asyncio
async def test_connection_manager_connect_and_limit():
    manager = ConnectionManager(max_connections=2)

    ws1 = MagicMock()
    ws1.accept = AsyncMock()
    ws1.close = AsyncMock()

    ws2 = MagicMock()
    ws2.accept = AsyncMock()
    ws2.close = AsyncMock()

    ws3 = MagicMock()
    ws3.accept = AsyncMock()
    ws3.close = AsyncMock()

    assert await manager.connect(ws1) is True
    assert await manager.connect(ws2) is True
    # Exceeds max_connections
    assert await manager.connect(ws3) is False
    ws3.close.assert_called_once_with(code=1008, reason="Connection limit exceeded")

    await manager.disconnect(ws1)
    assert len(manager.active_connections) == 1

    # Now ws3 can connect
    assert await manager.connect(ws3) is True
    assert len(manager.active_connections) == 2


@pytest.mark.asyncio
async def test_connection_manager_broadcast():
    manager = ConnectionManager()
    ws = MagicMock()
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()

    await manager.connect(ws)
    await manager.broadcast({"type": "test_msg"})

    ws.send_text.assert_called_once()
    assert '"type": "test_msg"' in ws.send_text.call_args[0][0]


@pytest.mark.asyncio
async def test_build_dashboard_payload():
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[
        500,  # total_today
        120,  # active_users
        25,   # count_5s -> 5.0 eps
        10, 5, 2, 1, # types
        12, 6,       # sources
        [("tech", 40.0)], # trending
        ['{"event_id": "e1", "event_type": "POST_CREATED"}'], # recent events
        ['{"anomaly_type": "SPIKE", "severity": "WARNING"}'], # recent anomalies
    ])
    mock_redis.pipeline.return_value = mock_pipe

    payload = await build_dashboard_payload(mock_redis)
    assert payload["type"] == "dashboard_update"
    assert payload["metrics"]["events_per_second"] == 5.0
    assert payload["metrics"]["total_events_today"] == 500
    assert payload["metrics"]["active_users"] == 120
    assert len(payload["trending"]) == 1
    assert payload["trending"][0]["community_id"] == "tech"
    assert len(payload["recent_events"]) == 1
    assert len(payload["recent_anomalies"]) == 1
