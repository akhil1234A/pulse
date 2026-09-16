"""Unit tests for BaseWorker framework."""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from src.common.config import Settings
from src.common.models import EventSource, EventType, PulseStreamEvent
from src.workers.base import BaseWorker


class DummyWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(worker_type="dummy", group_id="test-group", health_port=9999, **kwargs)
        self.processed_batches = []

    async def process_batch(self, events):
        self.processed_batches.append(events)


@pytest.fixture
def sample_event():
    return PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.SIMULATOR,
        user_id="user_123",
        community_id="tech",
    )


@pytest.mark.asyncio
async def test_worker_initialization():
    worker = DummyWorker()
    assert worker.worker_type == "dummy"
    assert worker.group_id == "test-group"
    assert worker.health_port == 9999
    assert worker._running is False


@pytest.mark.asyncio
async def test_check_dedup_flow():
    worker = DummyWorker()
    mock_redis = AsyncMock()
    worker._redis_client = mock_redis

    # First call: not duplicate (set returns True)
    mock_redis.set.return_value = True
    is_dup = await worker.check_dedup("evt-123")
    assert is_dup is False

    # Second call: duplicate (set returns False)
    mock_redis.set.return_value = False
    is_dup = await worker.check_dedup("evt-123")
    assert is_dup is True


@pytest.mark.asyncio
async def test_check_dedup_redis_degraded():
    worker = DummyWorker()
    worker._redis_client = None  # Redis unavailable
    # Should gracefully allow event through
    is_dup = await worker.check_dedup("evt-999")
    assert is_dup is False


@pytest.mark.asyncio
async def test_route_to_dlq():
    worker = DummyWorker()
    worker._db_pool = MagicMock()
    worker._producer = MagicMock()

    with patch("src.workers.base.insert_dead_letter_event", new=AsyncMock(return_value=1)) as mock_insert:
        await worker.route_to_dlq(
            raw_data={"invalid": True},
            error_msg="Test error",
            error_type="TEST_ERROR",
        )
        mock_insert.assert_called_once()
        worker._producer.produce.assert_called_once()


@pytest.mark.asyncio
async def test_handle_health():
    worker = DummyWorker()
    worker._running = True
    worker._redis_client = AsyncMock()
    worker._redis_client.ping.return_value = True

    req = MagicMock()
    resp = await worker._handle_health(req)
    assert resp.status == 200
    assert "healthy" in resp.text
