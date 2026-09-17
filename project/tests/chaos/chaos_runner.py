"""ChaosRunner framework for resilience and chaos engineering tests.

Manages Docker container states (pause, unpause, kill, restart) and injects
real-world faults (duplicate event floods, malformed/corrupt payloads, service outages).
"""

import json
import logging
import subprocess
import time
import uuid
from typing import List, Optional

import asyncpg
import redis.asyncio as aioredis

from src.common.config import get_settings
from src.common.kafka_utils import create_producer, produce_event
from src.common.models import EventSource, EventType, PulseStreamEvent

logger = logging.getLogger(__name__)

SERVICE_CONTAINER_MAP = {
    "agg-worker": "pulsestream-agg-worker",
    "anomaly-worker": "pulsestream-anomaly-worker",
    "redis": "pulsestream-redis",
    "postgresql": "pulsestream-postgres",
    "kafka": "pulsestream-kafka",
    "ingestion": "pulsestream-ingestion",
    "api-server": "pulsestream-api-server",
    "grafana": "pulsestream-grafana",
    "prometheus": "pulsestream-prometheus",
}


class ChaosRunner:
    """Controls Docker containers and performs fault injection for chaos experiments."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._producer = None

    def _resolve_name(self, service: str) -> str:
        """Resolves friendly service name to Docker container name."""
        return SERVICE_CONTAINER_MAP.get(service.lower(), service)

    def _run_docker_cmd(self, args: List[str]) -> subprocess.CompletedProcess:
        """Executes a Docker command synchronously."""
        cmd = ["docker"] + args
        logger.info("Executing Docker command: %s", " ".join(cmd))
        return subprocess.run(cmd, capture_output=True, text=True, check=False)

    def kill_container(self, service: str) -> None:
        """Kills the specified service container immediately."""
        container = self._resolve_name(service)
        logger.warning("Chaos: Killing container %s", container)
        res = self._run_docker_cmd(["kill", container])
        if res.returncode != 0:
            logger.warning("docker kill stderr: %s", res.stderr.strip())

    def pause_container(self, service: str) -> None:
        """Pauses the specified service container."""
        container = self._resolve_name(service)
        logger.warning("Chaos: Pausing container %s", container)
        res = self._run_docker_cmd(["pause", container])
        if res.returncode != 0:
            logger.warning("docker pause stderr: %s", res.stderr.strip())

    def unpause_container(self, service: str) -> None:
        """Unpauses a previously paused container."""
        container = self._resolve_name(service)
        logger.info("Chaos: Unpausing container %s", container)
        res = self._run_docker_cmd(["unpause", container])
        if res.returncode != 0:
            logger.warning("docker unpause stderr: %s", res.stderr.strip())

    def restart_container(self, service: str) -> None:
        """Restarts (or starts) the specified service container."""
        container = self._resolve_name(service)
        logger.info("Chaos: Restarting container %s", container)
        res = self._run_docker_cmd(["restart", container])
        if res.returncode != 0:
            logger.warning("docker restart stderr: %s", res.stderr.strip())

    def is_container_running(self, service: str) -> bool:
        """Checks if the container is currently in running state."""
        container = self._resolve_name(service)
        res = self._run_docker_cmd(["inspect", "-f", "{{.State.Running}}", container])
        return res.stdout.strip().lower() == "true"

    def is_container_paused(self, service: str) -> bool:
        """Checks if the container is currently paused."""
        container = self._resolve_name(service)
        res = self._run_docker_cmd(["inspect", "-f", "{{.State.Paused}}", container])
        return res.stdout.strip().lower() == "true"

    def restore_all_containers(self) -> None:
        """Safety restoration: unpauses and starts any affected containers."""
        logger.info("Restoring all containers to healthy state...")
        for service, container in SERVICE_CONTAINER_MAP.items():
            try:
                # If paused, unpause
                if self.is_container_paused(container):
                    self.unpause_container(container)
                # If not running, start
                if not self.is_container_running(container):
                    self._run_docker_cmd(["start", container])
            except Exception as e:
                logger.error("Error restoring %s: %s", container, e)

    def inject_duplicates(
        self,
        count: int = 100,
        target_event: Optional[PulseStreamEvent] = None,
    ) -> PulseStreamEvent:
        """Injects `count` identical copies of an event with the exact same event_id."""
        if target_event is None:
            now_ms = int(time.time() * 1000)
            target_event = PulseStreamEvent(
                event_id=str(uuid.uuid4()),
                event_type=EventType.POST_CREATED,
                source=EventSource.SIMULATOR,
                timestamp=now_ms,
                user_id="chaos_user_001",
                community_id="technology",
                payload={"chaos_test": "duplicate_dedup", "seed": time.time()},
                ingested_at=now_ms,
                schema_version="1.0",
            )

        producer = create_producer()
        logger.info(
            "Chaos: Injecting %d duplicates of event_id=%s into topic=%s",
            count,
            target_event.event_id,
            self.settings.kafka_events_topic,
        )

        for _ in range(count):
            produce_event(
                producer=producer,
                topic=self.settings.kafka_events_topic,
                event=target_event,
            )
        producer.flush(timeout=10.0)
        return target_event

    def inject_malformed(self, count: int = 50) -> List[str]:
        """Injects `count` corrupt, invalid payloads directly into Kafka raw topic."""
        producer = create_producer()
        logger.info(
            "Chaos: Injecting %d malformed payloads into topic=%s",
            count,
            self.settings.kafka_events_topic,
        )

        injected_ids = []
        for i in range(count):
            err_id = f"malformed-{uuid.uuid4()}"
            injected_ids.append(err_id)
            # Create intentionally unparseable bytes or schema-violating JSON
            if i % 2 == 0:
                payload = b"\x00\xff\xfeINVALID_AVRO_BYTES_HEADER" + err_id.encode("utf-8")
            else:
                payload = json.dumps(
                    {
                        "corrupt_id": err_id,
                        "bad_field": 12345,
                        "missing": "everything_required",
                    }
                ).encode("utf-8")

            producer.produce(
                topic=self.settings.kafka_events_topic,
                key=err_id.encode("utf-8"),
                value=payload,
            )
            producer.poll(0)

        producer.flush(timeout=10.0)
        return injected_ids

    async def get_postgres_event_count(self, event_id: str) -> int:
        """Queries PostgreSQL to count occurrences of a specific event_id."""
        dsn = self.settings.postgres_dsn
        conn = await asyncpg.connect(dsn)
        try:
            val = await conn.fetchval(
                "SELECT COUNT(*) FROM events WHERE event_id = $1",
                uuid.UUID(event_id),
            )
            return int(val or 0)
        finally:
            await conn.close()

    async def get_postgres_dlq_count(self) -> int:
        """Queries PostgreSQL to count total records in dead_letter_events table."""
        dsn = self.settings.postgres_dsn
        conn = await asyncpg.connect(dsn)
        try:
            val = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_events")
            return int(val or 0)
        finally:
            await conn.close()

    async def get_redis_dedup_status(self, event_id: str) -> bool:
        """Checks if a dedup key exists in Redis."""
        client = aioredis.from_url(self.settings.redis_url, decode_responses=True)
        try:
            return bool(await client.exists(f"ps:dedup:{event_id}"))
        finally:
            await client.aclose()
