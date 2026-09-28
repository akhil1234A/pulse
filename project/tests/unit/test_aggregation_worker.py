"""Unit tests for AggregationWorker."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.config import Settings
from src.common.models import EventSource, EventType, PulseStreamEvent
from src.workers.aggregation.aggregator import AggregationWorker


@pytest.fixture
def sample_events():
    return [
        PulseStreamEvent(
            event_id=f"evt-{i}",
            event_type=EventType.POST_CREATED,
            source=EventSource.WIKIPEDIA,
            user_id=f"user_{i}",
            community_id="en.wikipedia.org" if i % 2 == 0 else None,
        )
        for i in range(5)
    ]


@pytest.mark.asyncio
async def test_update_redis_aggregates(sample_events):
    worker = AggregationWorker()
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock()
    mock_redis.pipeline.return_value.__aenter__.return_value = mock_pipe
    worker._redis_client = mock_redis

    await worker._update_redis_aggregates(sample_events)

    # Verify pipeline execute was called
    mock_pipe.execute.assert_called_once()
    # Check that incr, pfadd, and zadd were called
    assert mock_pipe.incr.call_count >= 5
    assert mock_pipe.pfadd.call_count == 5
    assert mock_pipe.zadd.call_count == 5


@pytest.mark.asyncio
async def test_process_batch_dedup_and_buffer(sample_events):
    worker = AggregationWorker()
    worker._redis_client = AsyncMock()
    worker._update_redis_aggregates = AsyncMock()
    worker.flush_pg_buffer = AsyncMock()

    # Make first event duplicate, others clean
    async def mock_check_dedup(event_id, ttl_seconds=3600):
        return event_id == "evt-0"

    worker.check_dedup = mock_check_dedup

    await worker.process_batch(sample_events)

    # 4 clean events should be passed to aggregates and buffer
    assert len(worker._pg_buffer) == 4
    worker._update_redis_aggregates.assert_called_once()
    flushed_batch = worker._update_redis_aggregates.call_args[0][0]
    assert len(flushed_batch) == 4
    assert all(ev.event_id != "evt-0" for ev in flushed_batch)


@pytest.mark.asyncio
async def test_process_batch_invokes_intelligence_detector(sample_events):
    worker = AggregationWorker()
    worker._redis_client = AsyncMock()
    worker._update_redis_aggregates = AsyncMock()
    worker.flush_pg_buffer = AsyncMock()
    worker.check_dedup = AsyncMock(return_value=False)
    worker.intelligence_detector.evaluate_event = AsyncMock(return_value=[])

    await worker.process_batch(sample_events)

    assert worker.intelligence_detector.evaluate_event.call_count == len(sample_events)


@pytest.mark.asyncio
async def test_flush_pg_buffer_size_threshold(sample_events):
    settings = Settings(worker_batch_flush_size=2)
    worker = AggregationWorker(settings=settings)
    worker._db_pool = MagicMock()
    worker._pg_buffer = list(sample_events)  # 5 items > threshold of 2

    with patch(
        "src.workers.aggregation.aggregator.batch_insert_events", new=AsyncMock(return_value=5)
    ) as mock_insert:
        await worker.flush_pg_buffer(force=False)
        mock_insert.assert_called_once()
        assert len(worker._pg_buffer) == 0


@pytest.mark.asyncio
async def test_flush_pg_buffer_circuit_breaker_open(sample_events):
    worker = AggregationWorker()
    worker._db_pool = MagicMock()
    worker._pg_buffer = list(sample_events)
    # Trip circuit breaker
    worker.pg_circuit._transition_to(worker.pg_circuit.state.OPEN)

    with patch(
        "src.workers.aggregation.aggregator.batch_insert_events", new=AsyncMock()
    ) as mock_insert:
        await worker.flush_pg_buffer(force=False)
        mock_insert.assert_not_called()
        # Buffer should be retained
        assert len(worker._pg_buffer) == len(sample_events)
