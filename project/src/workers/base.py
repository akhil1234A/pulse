"""Base Worker Framework for PulseStream.

Provides a robust Kafka consumer loop with manual offset management,
deduplication hooks, DLQ routing, Prometheus metrics, health check endpoints,
and graceful shutdown.
"""

import asyncio
import logging
import signal
import sys
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from aiohttp import web
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from src.common.config import Settings, get_settings
from src.common.db import close_db_pool, create_db_pool, insert_dead_letter_event
from src.common.kafka_utils import create_consumer, create_producer, produce_event
from src.common.logging import get_logger, set_correlation_id
from src.common.metrics import (
    WORKER_BATCH_SIZE,
    WORKER_EVENTS_PROCESSED_TOTAL,
    WORKER_PROCESSING_LATENCY_SECONDS,
    start_metrics_exporter,
)
from src.common.models import PulseStreamEvent
from src.common.redis_utils import (
    check_and_set_dedup_async,
    get_async_redis_client,
)
from src.common.retry import CircuitBreaker

logger = get_logger(__name__)


class BaseWorker(ABC):
    """Abstract base worker implementing Kafka polling, error handling, and lifecycle."""

    def __init__(
        self,
        worker_type: str,
        group_id: str,
        topics: Optional[List[str]] = None,
        health_port: Optional[int] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        self.worker_type = worker_type
        self.group_id = group_id
        self.settings = settings or get_settings()
        self.topics = topics or [self.settings.kafka_events_topic]
        self.health_port = health_port or self.settings.worker_health_port

        self._running = False
        self._consumer: Optional[Consumer] = None
        self._producer: Optional[Any] = None  # for DLQ publishing
        self._db_pool: Optional[Any] = None
        self._redis_client: Optional[Any] = None
        self._health_runner: Optional[web.AppRunner] = None

        # Circuit breakers for external resources
        self.pg_circuit = CircuitBreaker("postgresql")
        self.redis_circuit = CircuitBreaker("redis")

    async def initialize(self) -> None:
        """Initializes database pool, redis client, kafka consumer and producer."""
        logger.info("Initializing %s worker resources...", self.worker_type)

        # 1. Async Redis client
        try:
            self._redis_client = get_async_redis_client(self.settings.redis_url)
            await self._redis_client.ping()
            logger.info("Connected to Redis successfully.")
        except Exception as e:
            logger.warning("Initial Redis connection failed (will retry dynamically): %s", e)

        # 2. Database connection pool
        try:
            self._db_pool = await create_db_pool(self.settings.postgres_dsn)
            logger.info("Connected to PostgreSQL successfully.")
        except Exception as e:
            logger.warning("Initial PostgreSQL pool creation failed: %s", e)

        # 3. Kafka DLQ Producer
        self._producer = create_producer(self.settings.kafka_bootstrap_servers)

        # 4. Kafka Consumer
        self._consumer = create_consumer(
            group_id=self.group_id,
            topics=self.topics,
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

        # 5. Health check web endpoint
        await self._start_health_server()

        # 6. Prometheus exporter if configured on distinct port
        if self.settings.metrics_port != self.health_port:
            start_metrics_exporter(self.settings.metrics_port)

    async def _start_health_server(self) -> None:
        """Starts a lightweight HTTP server exposing /health and /metrics."""
        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        app.router.add_get("/metrics", self._handle_metrics)

        self._health_runner = web.AppRunner(app)
        await self._health_runner.setup()
        site = web.TCPSite(self._health_runner, "0.0.0.0", self.health_port)
        try:
            await site.start()
            logger.info("%s health and metrics check listening on port %d", self.worker_type, self.health_port)
        except Exception as e:
            logger.warning("Failed to bind health check port %d: %s", self.health_port, e)

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        """Prometheus metrics scrape endpoint."""
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
        return web.Response(body=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})

    async def _handle_health(self, request: web.Request) -> web.Response:
        """Health check handler returning worker status and dependency state."""
        redis_ok = False
        if self._redis_client:
            try:
                await self._redis_client.ping()
                redis_ok = True
            except Exception:
                pass

        return web.json_response({
            "status": "healthy" if self._running else "stopped",
            "worker_type": self.worker_type,
            "group_id": self.group_id,
            "redis_connected": redis_ok,
            "pg_circuit": self.pg_circuit.state.name,
            "redis_circuit": self.redis_circuit.state.name,
        })

    def _poll_raw_messages(self, max_batch_size: int, timeout: float) -> List[Message]:
        """Polls Kafka consumer synchronously in non-blocking manner to build a batch."""
        messages: List[Message] = []
        start_time = time.monotonic()

        while len(messages) < max_batch_size:
            remaining_time = max(0.0, timeout - (time.monotonic() - start_time))
            if remaining_time <= 0 and messages:
                break

            msg = self._consumer.poll(timeout=min(remaining_time, 0.2))
            if msg is None:
                if messages:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka poll error: %s", msg.error())
                continue

            messages.append(msg)
            if (time.monotonic() - start_time) >= timeout:
                break

        return messages

    async def route_to_dlq(self, raw_data: Any, error_msg: str, error_type: str = "WORKER_ERROR") -> None:
        """Routes a malformed or failed event to DLQ topic and dead_letter_events table."""
        logger.error("Routing event to DLQ: error_type=%s, msg=%s", error_type, error_msg)
        if self._db_pool:
            try:
                await insert_dead_letter_event(
                    pool=self._db_pool,
                    raw_event=raw_data,
                    error_message=error_msg,
                    error_type=error_type,
                )
            except Exception as e:
                logger.error("Failed to insert DLQ record to PostgreSQL: %s", e)

        # Produce raw message to DLQ topic if available
        if self._producer:
            try:
                payload = raw_data if isinstance(raw_data, (str, bytes)) else str(raw_data)
                if isinstance(payload, str):
                    payload = payload.encode("utf-8")
                self._producer.produce(
                    topic=self.settings.kafka_dlq_topic,
                    value=payload,
                )
                self._producer.poll(0)
            except Exception as e:
                logger.error("Failed to produce event to Kafka DLQ topic: %s", e)

    async def check_dedup(self, event_id: str, ttl_seconds: int = 3600) -> bool:
        """Checks if event_id is duplicate using Redis.

        Returns True if event was already processed (duplicate), False otherwise.
        """
        if not self._redis_client or not self.redis_circuit.is_call_permitted():
            # Graceful degradation: allow event through if Redis is unavailable
            return False

        prefix = "ps:dedup" if self.worker_type == "aggregation" else f"ps:dedup:{self.worker_type}"
        try:
            return await asyncio.wait_for(
                check_and_set_dedup_async(
                    self._redis_client,
                    event_id,
                    ttl_seconds=ttl_seconds,
                    prefix=prefix,
                ),
                timeout=1.5,
            )
        except Exception as e:
            logger.warning("Redis dedup check error (degraded to allow): %s", e)
            self.redis_circuit.record_failure()
            return False

    @abstractmethod
    async def process_batch(self, events: List[PulseStreamEvent]) -> None:
        """Subclass implementation to process a batch of validated events."""
        pass

    async def run(self) -> None:
        """Main execution loop for worker."""
        await self.initialize()
        self._running = True
        logger.info("Starting worker loop for %s...", self.worker_type)

        loop = asyncio.get_running_loop()

        while self._running:
            try:
                # 1. Poll batch from Kafka via executor to avoid blocking asyncio loop
                messages = await loop.run_in_executor(
                    None,
                    self._poll_raw_messages,
                    self.settings.worker_max_batch_size,
                    self.settings.worker_poll_timeout_seconds,
                )

                if not messages:
                    await asyncio.sleep(0.05)
                    continue

                batch_start = time.monotonic()
                valid_events: List[PulseStreamEvent] = []

                # 2. Deserialize messages
                for msg in messages:
                    val = msg.value()
                    try:
                        event = PulseStreamEvent.from_avro_bytes(val)
                        valid_events.append(event)
                    except Exception as exc:
                        raw_str = val.decode("utf-8", errors="replace") if val else ""
                        await self.route_to_dlq(raw_str, f"Deserialization error: {exc}", "DESERIALIZE_ERROR")

                # 3. Process batch in subclass
                if valid_events:
                    WORKER_BATCH_SIZE.labels(worker_type=self.worker_type).observe(len(valid_events))
                    await self.process_batch(valid_events)
                    WORKER_EVENTS_PROCESSED_TOTAL.labels(worker_type=self.worker_type).inc(len(valid_events))

                # 4. Commit Kafka offsets synchronously
                if self._consumer and self._running:
                    await loop.run_in_executor(None, lambda: self._consumer.commit(asynchronous=False))

                latency = time.monotonic() - batch_start
                WORKER_PROCESSING_LATENCY_SECONDS.labels(worker_type=self.worker_type).observe(latency)

            except Exception as exc:
                if not self._running:
                    break
                logger.error("Unhandled error in %s loop: %s", self.worker_type, exc, exc_info=True)
                await asyncio.sleep(1.0)

        logger.info("Worker loop stopped for %s.", self.worker_type)
        await self.shutdown()

    async def shutdown(self) -> None:
        """Gracefully shuts down worker resources."""
        self._running = False
        logger.info("Shutting down %s worker...", self.worker_type)

        if self._consumer:
            try:
                self._consumer.close()
                logger.info("Kafka consumer closed.")
            except Exception as e:
                logger.warning("Error closing Kafka consumer: %s", e)

        if self._producer:
            try:
                self._producer.flush(timeout=5.0)
                logger.info("Kafka producer flushed.")
            except Exception as e:
                logger.warning("Error flushing Kafka producer: %s", e)

        if self._redis_client:
            try:
                await self._redis_client.aclose()
                logger.info("Redis client closed.")
            except Exception as e:
                logger.warning("Error closing Redis client: %s", e)

        if self._db_pool:
            try:
                await close_db_pool(self._db_pool)
                logger.info("PostgreSQL pool closed.")
            except Exception as e:
                logger.warning("Error closing PostgreSQL pool: %s", e)

        if self._health_runner:
            try:
                await self._health_runner.cleanup()
                logger.info("Health server cleaned up.")
            except Exception as e:
                logger.warning("Error cleaning up health server: %s", e)
