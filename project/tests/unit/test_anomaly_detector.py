"""Unit tests for AnomalyDetectionWorker."""

from unittest.mock import AsyncMock, MagicMock
import pytest

from src.common.models import EventSource, EventType, PulseStreamEvent
from src.workers.anomaly.detector import AnomalyDetectionWorker
from src.workers.anomaly.rules import AnomalyRecord


@pytest.fixture
def sample_events():
    return [
        PulseStreamEvent(
            event_id=f"evt-{i}",
            event_type=EventType.POST_CREATED,
            source=EventSource.SIMULATOR,
            user_id=f"user_{i}",
            community_id="science",
        )
        for i in range(12)
    ]


@pytest.mark.asyncio
async def test_record_anomaly_db_and_redis():
    worker = AnomalyDetectionWorker()

    mock_db = MagicMock()
    mock_conn = AsyncMock()
    mock_db.acquire.return_value.__aenter__.return_value = mock_conn
    mock_conn.fetchval.return_value = 1
    worker._db_pool = mock_db

    mock_redis = AsyncMock()
    worker._redis_client = mock_redis

    record = AnomalyRecord(
        anomaly_type="USER_FLOOD",
        severity="HIGH",
        description="User flood test",
        details={"user_id": "u1", "count": 60},
        event_ids=["00000000-0000-0000-0000-000000000001"],
    )

    await worker._record_anomaly(record)

    # Verify DB insertion
    mock_conn.fetchval.assert_called_once()
    # Verify Redis list push and publish
    mock_redis.lpush.assert_called_once()
    mock_redis.publish.assert_called_once()


@pytest.mark.asyncio
async def test_process_batch_with_duplicate_storm(sample_events):
    worker = AnomalyDetectionWorker()
    worker._record_anomaly = AsyncMock()

    # Make 2 out of 12 events duplicates (2/12 = 16.7% > 5%)
    async def mock_check_dedup(event_id, ttl_seconds=3600):
        return event_id in ("evt-0", "evt-1")

    worker.check_dedup = mock_check_dedup

    await worker.process_batch(sample_events)

    # Verify _record_anomaly was called for duplicate storm
    called_records = [call[0][0] for call in worker._record_anomaly.call_args_list]
    storm_records = [r for r in called_records if r.anomaly_type == "DUPLICATE_STORM"]
    assert len(storm_records) == 1
    assert storm_records[0].severity == "HIGH"
