"""Anomaly Detection Worker implementation for PulseStream."""

import json
from typing import List, Optional
import uuid

from src.common.config import Settings, get_settings
from src.common.logging import get_logger
from src.common.metrics import ANOMALY_DETECTED_TOTAL
from src.common.models import PulseStreamEvent
from src.workers.anomaly.rules import (
    AnomalyRecord,
    BaseAnomalyRule,
    CommunitySpikeRule,
    DuplicateStormRule,
    UserFloodRule,
)
from src.workers.base import BaseWorker

logger = get_logger(__name__)


class AnomalyDetectionWorker(BaseWorker):
    """Worker evaluating streaming events against anomaly detection rules."""

    def __init__(
        self,
        group_id: str = "ps-anomaly",
        topics: Optional[List[str]] = None,
        health_port: Optional[int] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        super().__init__(
            worker_type="anomaly",
            group_id=group_id,
            topics=topics,
            health_port=health_port or 9102,
            settings=settings,
        )
        self.event_rules: List[BaseAnomalyRule] = [
            UserFloodRule(threshold=50, window_seconds=60),
            CommunitySpikeRule(multiplier_threshold=10.0, min_events=10),
        ]
        self.duplicate_storm_rule = DuplicateStormRule(threshold_ratio=0.05, min_batch_size=10)

    async def _record_anomaly(self, record: AnomalyRecord) -> None:
        """Stores anomaly in PostgreSQL and publishes notification to Redis."""
        logger.warning(
            "Anomaly detected: type=%s, severity=%s, description=%s",
            record.anomaly_type,
            record.severity,
            record.description,
        )

        # 1. Increment Prometheus metric
        try:
            ANOMALY_DETECTED_TOTAL.labels(
                anomaly_type=record.anomaly_type,
                severity=record.severity,
            ).inc()
        except Exception:
            pass

        # 2. Record to PostgreSQL anomalies table
        if self._db_pool and self.pg_circuit.is_call_permitted():
            try:
                # Convert string event_ids to uuid.UUID objects for PostgreSQL UUID[]
                uuid_list = []
                for eid in record.event_ids:
                    try:
                        uuid_list.append(uuid.UUID(str(eid)))
                    except ValueError:
                        pass

                details_json = json.dumps(record.details)
                query = """
                    INSERT INTO anomalies (anomaly_type, severity, description, details, event_ids)
                    VALUES ($1, $2, $3, $4::jsonb, $5)
                    RETURNING id;
                """
                async with self._db_pool.acquire() as conn:
                    await conn.fetchval(
                        query,
                        record.anomaly_type,
                        record.severity,
                        record.description,
                        details_json,
                        uuid_list,
                    )
                self.pg_circuit.record_success()
            except Exception as e:
                logger.error("Failed to persist anomaly to database: %s", e)
                self.pg_circuit.record_failure()

        # 3. Publish to Redis for real-time dashboard notification
        if self._redis_client and self.redis_circuit.is_call_permitted():
            try:
                payload = json.dumps({
                    "anomaly_type": record.anomaly_type,
                    "severity": record.severity,
                    "description": record.description,
                    "details": record.details,
                    "event_ids": record.event_ids,
                })
                # Maintain recent anomalies list (capped at 50)
                await self._redis_client.lpush("ps:anomalies:recent", payload)
                await self._redis_client.ltrim("ps:anomalies:recent", 0, 49)

                # Also publish to Redis channel for live WebSocket push
                await self._redis_client.publish("ps:channel:anomalies", payload)
                self.redis_circuit.record_success()
            except Exception as e:
                logger.warning("Failed to publish anomaly to Redis: %s", e)
                self.redis_circuit.record_failure()

    async def process_batch(self, events: List[PulseStreamEvent]) -> None:
        """Processes a batch of events: checks duplicates and evaluates anomaly rules."""
        if not events:
            return

        duplicate_count = 0
        duplicate_event_ids: List[str] = []
        clean_events: List[PulseStreamEvent] = []

        # Dedup check
        for ev in events:
            is_dup = await self.check_dedup(ev.event_id, ttl_seconds=3600)
            if is_dup:
                duplicate_count += 1
                duplicate_event_ids.append(ev.event_id)
            else:
                clean_events.append(ev)

        # 1. Evaluate batch-level Duplicate Storm rule
        dup_anomaly = self.duplicate_storm_rule.evaluate_batch(
            duplicate_count=duplicate_count,
            total_count=len(events),
            duplicate_event_ids=duplicate_event_ids,
        )
        if dup_anomaly:
            await self._record_anomaly(dup_anomaly)

        # 2. Evaluate individual event rules (flood, spike) on clean events
        for ev in clean_events:
            for rule in self.event_rules:
                anomaly = await rule.evaluate_event(ev, self._redis_client)
                if anomaly:
                    await self._record_anomaly(anomaly)
