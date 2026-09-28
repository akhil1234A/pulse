"""Aggregation worker implementation for real-time counters, trending, and durable storage."""

import asyncio
import time
from typing import List, Optional

from src.common.config import Settings
from src.common.db import batch_insert_events
from src.common.logging import get_logger
from src.common.metrics import (
    REDIS_DEDUP_HITS_TOTAL,
    STORAGE_BATCH_INSERT_LATENCY_SECONDS,
    STORAGE_ROWS_INSERTED_TOTAL,
)
from src.common.models import PulseStreamEvent
from src.common.redis_utils import (
    active_users_key,
    community_count_key,
    events_per_sec_key,
    events_today_key,
    recent_events_key,
    source_count_key,
    trending_communities_key,
    type_count_key,
)
from src.workers.base import BaseWorker
from src.workers.intelligence.cross_source import CrossSourceIntelligenceDetector

logger = get_logger(__name__)


class AggregationWorker(BaseWorker):
    """Worker responsible for updating Redis aggregates and flushing
    events to PostgreSQL in batches."""

    def __init__(
        self,
        group_id: str = "ps-aggregation",
        topics: Optional[List[str]] = None,
        health_port: Optional[int] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        super().__init__(
            worker_type="aggregation",
            group_id=group_id,
            topics=topics,
            health_port=health_port or 9101,
            settings=settings,
        )
        self._pg_buffer: List[PulseStreamEvent] = []
        self._last_flush_time: float = time.monotonic()
        self._flush_lock = asyncio.Lock()
        self.intelligence_detector = CrossSourceIntelligenceDetector()

    async def _update_redis_aggregates(self, events: List[PulseStreamEvent]) -> None:
        """Executes batched Redis operations via pipeline."""
        if not self._redis_client or not self.redis_circuit.is_call_permitted():
            return

        try:
            async with self._redis_client.pipeline(transaction=False) as pipe:
                now_ts = int(time.time())
                cutoff_ts = now_ts - 60

                for ev in events:
                    # 1. Total events today (24h TTL)
                    pipe.incr(events_today_key())
                    pipe.expire(events_today_key(), 86400)

                    # 2. Event type counter
                    type_key = type_count_key(ev.event_type.value)
                    pipe.incr(type_key)
                    pipe.expire(type_key, 86400)

                    # 3. Source counter
                    source_key = source_count_key(ev.source.value)
                    pipe.incr(source_key)
                    pipe.expire(source_key, 86400)

                    # 4. Community counter and trending score
                    if ev.community_id:
                        comm_key = community_count_key(ev.community_id)
                        pipe.incr(comm_key)
                        pipe.expire(comm_key, 86400)

                        pipe.zincrby(trending_communities_key(), 1.0, ev.community_id)
                        pipe.expire(trending_communities_key(), 3600)

                    # 5. Active users via HyperLogLog
                    pipe.pfadd(active_users_key(), ev.user_id)

                    # 6. Events per second rate tracker (timestamp -> count)
                    pipe.zadd(events_per_sec_key(), {f"{ev.event_id}:{now_ts}": now_ts})

                    # 7. Recent events list for dashboard
                    pipe.lpush(recent_events_key(), ev.model_dump_json())

                # Trim events older than 60s from rate tracker
                pipe.zremrangebyscore(events_per_sec_key(), 0, cutoff_ts)
                # Trim recent events list to latest 50
                pipe.ltrim(recent_events_key(), 0, 49)

                await asyncio.wait_for(pipe.execute(), timeout=1.5)
                self.redis_circuit.record_success()

        except Exception as e:
            logger.warning("Redis aggregation update failed: %s", e)
            self.redis_circuit.record_failure()

    async def flush_pg_buffer(self, force: bool = False) -> None:
        """Flushes buffered events to PostgreSQL using COPY protocol."""
        async with self._flush_lock:
            buffer_len = len(self._pg_buffer)
            if buffer_len == 0:
                return

            time_since_last = time.monotonic() - self._last_flush_time
            should_flush = (
                force
                or buffer_len >= self.settings.worker_batch_flush_size
                or time_since_last >= self.settings.worker_batch_flush_interval_seconds
            )

            if not should_flush:
                return

            if not self._db_pool:
                logger.warning(
                    "PostgreSQL pool not available; delaying flush of %d events.", buffer_len
                )
                return

            if not self.pg_circuit.is_call_permitted():
                logger.warning(
                    "PostgreSQL circuit breaker is OPEN; retaining %d events in memory buffer.",
                    buffer_len,
                )
                return

            to_insert = list(self._pg_buffer)
            t0 = time.monotonic()

            try:
                inserted_count = await batch_insert_events(self._db_pool, to_insert)
                elapsed = time.monotonic() - t0

                STORAGE_BATCH_INSERT_LATENCY_SECONDS.observe(elapsed)
                STORAGE_ROWS_INSERTED_TOTAL.inc(inserted_count)
                self.pg_circuit.record_success()

                # Clear flushed events from buffer
                self._pg_buffer = self._pg_buffer[len(to_insert) :]
                self._last_flush_time = time.monotonic()
                logger.info(
                    "Flushed %d events to PostgreSQL in %.3fs (remaining buffer: %d)",
                    inserted_count,
                    elapsed,
                    len(self._pg_buffer),
                )
            except Exception as e:
                logger.error(
                    "Failed to batch insert %d events to PostgreSQL: %s", len(to_insert), e
                )
                self.pg_circuit.record_failure()

    async def process_batch(self, events: List[PulseStreamEvent]) -> None:
        """Processes a batch: deduplicates, updates aggregates, and buffers for DB flush."""
        new_events: List[PulseStreamEvent] = []

        for ev in events:
            # Dedup check
            is_dup = await self.check_dedup(ev.event_id, ttl_seconds=3600)
            if is_dup:
                REDIS_DEDUP_HITS_TOTAL.inc()
                logger.debug("Duplicate event %s skipped.", ev.event_id)
                continue
            new_events.append(ev)

        if not new_events:
            return

        # 1. Update Redis aggregates in bulk
        await self._update_redis_aggregates(new_events)

        # 2. Check for cross-source correlation signals (Wikipedia + Hacker News)
        for ev in new_events:
            try:
                await self.intelligence_detector.evaluate_event(
                    ev,
                    redis_client=self._redis_client,
                    db_pool=self._db_pool,
                    pg_circuit=self.pg_circuit,
                    redis_circuit=self.redis_circuit,
                )
            except Exception as e:
                logger.debug("Cross-source evaluation failed for event %s: %s", ev.event_id, e)

        # 3. Append to PostgreSQL buffer
        self._pg_buffer.extend(new_events)

        # 4. Flush to PostgreSQL before committing Kafka offsets
        await self.flush_pg_buffer(force=True)

    async def shutdown(self) -> None:
        """Flush remaining events before exiting."""
        logger.info("Draining PostgreSQL event buffer before worker shutdown...")
        try:
            await self.flush_pg_buffer(force=True)
        except Exception as e:
            logger.error("Error during final flush: %s", e)
        await super().shutdown()
