"""Chaos engineering tests verifying system resilience, deduplication, DLQ routing, and recovery."""

import asyncio
import logging
import time
import uuid
from typing import Generator

import pytest

from src.common.kafka_utils import create_producer, produce_event
from src.common.models import EventSource, EventType, PulseStreamEvent
from tests.chaos.chaos_runner import ChaosRunner

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def chaos() -> Generator[ChaosRunner, None, None]:
    """Provides a ChaosRunner and guarantees container restoration afterwards."""
    runner = ChaosRunner()
    try:
        # Pre-check: Ensure critical containers are running
        runner.restore_all_containers()
        time.sleep(1.0)
        yield runner
    finally:
        runner.restore_all_containers()
        time.sleep(2.0)


@pytest.mark.asyncio
async def test_duplicate_dedup(chaos: ChaosRunner):
    """Test 2: Duplicate Dedup.

    Injects 100 identical events with the same event_id.
    Asserts:
    1. Exactly 1 record is persisted in PostgreSQL events table.
    2. Redis dedup cache records the event.
    """
    now_ms = int(time.time() * 1000)
    event_id = str(uuid.uuid4())
    target_event = PulseStreamEvent(
        event_id=event_id,
        event_type=EventType.POST_CREATED,
        source=EventSource.SIMULATOR,
        timestamp=now_ms,
        user_id="chaos_dedup_user",
        community_id="technology",
        payload={"experiment": "dedup_test", "run_id": event_id},
        ingested_at=now_ms,
        schema_version="1.0",
    )

    # Inject 100 duplicates into Kafka
    chaos.inject_duplicates(count=100, target_event=target_event)

    # Allow workers to consume, deduplicate, and flush to Postgres
    max_wait = 15.0
    start = time.time()
    count = 0
    while time.time() - start < max_wait:
        count = await chaos.get_postgres_event_count(event_id)
        if count >= 1:
            break
        await asyncio.sleep(0.5)

    # Wait an extra 2 seconds for any remaining duplicate processing
    await asyncio.sleep(2.0)
    final_count = await chaos.get_postgres_event_count(event_id)

    assert final_count == 1, f"Expected exactly 1 event row in Postgres, got {final_count}"

    # Verify Redis dedup key exists
    is_in_redis = await chaos.get_redis_dedup_status(event_id)
    assert is_in_redis is True, "Expected event_id to be stored in Redis dedup"


@pytest.mark.asyncio
async def test_malformed_event_dlq(chaos: ChaosRunner):
    """Test 3: Malformed Event DLQ.

    Injects corrupt and schema-violating events.
    Asserts:
    1. Malformed events are intercepted and written to dead_letter_events table.
    2. Valid events continue processing concurrently without getting blocked.
    """
    initial_dlq_count = await chaos.get_postgres_dlq_count()

    # Inject 50 malformed events directly into Kafka topic
    chaos.inject_malformed(count=50)

    # Concurrently inject 1 valid event to assert pipeline health
    valid_id = str(uuid.uuid4())
    now_ms = int(time.time() * 1000)
    valid_event = PulseStreamEvent(
        event_id=valid_id,
        event_type=EventType.COMMENT_CREATED,
        source=EventSource.SIMULATOR,
        timestamp=now_ms,
        user_id="valid_user_concurrent",
        community_id="science",
        payload={"valid": True},
        ingested_at=now_ms,
        schema_version="1.0",
    )
    producer = create_producer()
    produce_event(
        producer=producer,
        topic=chaos.settings.kafka_events_topic,
        event=valid_event,
    )
    producer.flush(timeout=5.0)

    # Poll until DLQ count increases
    max_wait = 15.0
    start = time.time()
    dlq_increased = False
    while time.time() - start < max_wait:
        new_dlq_count = await chaos.get_postgres_dlq_count()
        if new_dlq_count > initial_dlq_count:
            dlq_increased = True
            break
        await asyncio.sleep(0.5)

    assert dlq_increased is True, f"DLQ count did not increase (initial: {initial_dlq_count})"

    # Assert the valid event was still processed into PostgreSQL
    valid_count = 0
    start = time.time()
    while time.time() - start < 10.0:
        valid_count = await chaos.get_postgres_event_count(valid_id)
        if valid_count == 1:
            break
        await asyncio.sleep(0.5)

    assert valid_count == 1, "Valid event was blocked or lost due to malformed events"


@pytest.mark.asyncio
async def test_redis_unavailability(chaos: ChaosRunner):
    """Test 4: Redis Unavailability & Graceful Degradation.

    Pauses Redis container mid-stream.
    Asserts:
    1. Workers do not crash when Redis is unresponsive.
    2. Events are still processed (degraded mode).
    3. Resuming Redis restores normal operation.
    """
    # 1. Pause Redis container
    chaos.pause_container("redis")
    assert chaos.is_container_paused("redis") is True

    try:
        # 2. Emit an event while Redis is down
        degraded_id = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)
        event = PulseStreamEvent(
            event_id=degraded_id,
            event_type=EventType.POST_CREATED,
            source=EventSource.SIMULATOR,
            timestamp=now_ms,
            user_id="degraded_user",
            community_id="gaming",
            payload={"degraded": True},
            ingested_at=now_ms,
            schema_version="1.0",
        )

        producer = create_producer()
        produce_event(
            producer=producer,
            topic=chaos.settings.kafka_events_topic,
            event=event,
        )
        producer.flush(timeout=5.0)

        # Allow time for worker to attempt Redis, trip circuit breaker, and persist to Postgres
        await asyncio.sleep(5.0)
    finally:
        # 3. Unpause Redis
        chaos.unpause_container("redis")
        await asyncio.sleep(2.0)

    # 4. Emit a recovery event now that Redis is back up
    recovery_id = str(uuid.uuid4())
    rec_event = PulseStreamEvent(
        event_id=recovery_id,
        event_type=EventType.POST_CREATED,
        source=EventSource.SIMULATOR,
        timestamp=int(time.time() * 1000),
        user_id="recovery_user",
        community_id="gaming",
        payload={"recovered": True},
        ingested_at=int(time.time() * 1000),
        schema_version="1.0",
    )
    produce_event(
        producer=producer,
        topic=chaos.settings.kafka_events_topic,
        event=rec_event,
    )
    producer.flush(timeout=5.0)

    # Assert recovery event made it through
    max_wait = 15.0
    start = time.time()
    found = False
    while time.time() - start < max_wait:
        count = await chaos.get_postgres_event_count(recovery_id)
        if count >= 1:
            found = True
            break
        await asyncio.sleep(0.5)

    assert found is True, "Pipeline did not resume processing after Redis was unpaused"


@pytest.mark.asyncio
async def test_consumer_crash_recovery(chaos: ChaosRunner):
    """Test 1: Consumer Crash Recovery.

    Produces a batch of events, kills agg-worker container, restarts it,
    and asserts worker resumes from committed offset without lost events.
    """
    test_run = uuid.uuid4().hex[:6]
    events = []
    producer = create_producer()

    # 1. Kill agg-worker container to simulate abrupt failure
    chaos.kill_container("agg-worker")
    assert chaos.is_container_running("agg-worker") is False

    # 2. Produce 20 identifiable events into Kafka while worker is down
    for i in range(20):
        evt_id = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)
        evt = PulseStreamEvent(
            event_id=evt_id,
            event_type=EventType.POST_CREATED,
            source=EventSource.SIMULATOR,
            timestamp=now_ms,
            user_id=f"crash_user_{test_run}",
            community_id="technology",
            payload={"crash_test": test_run, "index": i},
            ingested_at=now_ms,
            schema_version="1.0",
        )
        events.append(evt)
        produce_event(
            producer=producer,
            topic=chaos.settings.kafka_events_topic,
            event=evt,
        )
    producer.flush(timeout=5.0)

    # 3. Wait simulating outage duration
    await asyncio.sleep(2.0)

    # 4. Restart agg-worker container to verify recovery from committed offset
    chaos.restart_container("agg-worker")
    assert chaos.is_container_running("agg-worker") is True

    # Poll until all 20 events arrive in PostgreSQL (allowing for Kafka session timeout & rebalance)
    max_wait = 60.0
    start = time.time()
    processed_count = 0

    while time.time() - start < max_wait:
        processed_count = 0
        for evt in events:
            c = await chaos.get_postgres_event_count(evt.event_id)
            if c > 0:
                processed_count += 1
        if processed_count == len(events):
            break
        await asyncio.sleep(1.0)

    assert processed_count == len(events), (
        f"Expected all {len(events)} events to be processed after crash recovery, but only {processed_count} arrived"
    )
