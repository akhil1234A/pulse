"""Ingestion pipeline: normalize, Avro schema validation, Kafka publish, and DLQ routing."""

import json
import logging
import time
from typing import Any, Dict, Optional
import uuid

from confluent_kafka import Producer

from src.common.config import get_settings
from src.common.kafka_utils import create_producer, get_partition_key, produce_event
from src.common.logging import set_correlation_id
from src.common.metrics import (
    INGESTION_EVENTS_REJECTED_TOTAL,
    INGESTION_EVENTS_TOTAL,
)
from src.common.models import EventSource, EventType, PulseStreamEvent
from src.common.schemas.avro import validate_event_dict

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """Processes incoming events from any connector through normalization,

    Avro validation, Kafka publishing, and Dead Letter Queue (DLQ) routing.
    """

    def __init__(
        self,
        producer: Optional[Producer] = None,
        events_topic: Optional[str] = None,
        dlq_topic: Optional[str] = None,
    ) -> None:
        settings = get_settings()
        self.events_topic = events_topic or settings.kafka_events_topic
        self.dlq_topic = dlq_topic or settings.kafka_dlq_topic
        self.producer = producer or create_producer(
            bootstrap_servers=settings.kafka_bootstrap_servers
        )

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Ensures required envelope structure and assigns an event_id if absent."""
        event_dict = dict(raw)

        if not event_dict.get("event_id"):
            event_dict["event_id"] = str(uuid.uuid4())

        if "ingested_at" not in event_dict:
            event_dict["ingested_at"] = int(time.time() * 1000)

        if "schema_version" not in event_dict:
            event_dict["schema_version"] = "1.0"

        # Ensure payload is stringified JSON
        if "payload" in event_dict and not isinstance(event_dict["payload"], str):
            event_dict["payload"] = json.dumps(event_dict["payload"])

        return event_dict

    def validate_avro(self, event_dict: Dict[str, Any]) -> bool:
        """Validates the normalized dictionary against the official Avro schema."""
        return validate_event_dict(event_dict)

    def publish_to_dlq(
        self,
        raw_event: Any,
        error_message: str,
        error_type: str,
    ) -> None:
        """Routes a rejected or malformed event to the Dead Letter Queue Kafka topic."""
        dlq_payload = {
            "raw_event": raw_event,
            "error_message": error_message,
            "error_type": error_type,
            "timestamp": int(time.time() * 1000),
        }
        dlq_bytes = json.dumps(dlq_payload, default=str).encode("utf-8")
        key = str(uuid.uuid4())

        self.producer.produce(
            topic=self.dlq_topic,
            key=key.encode("utf-8"),
            value=dlq_bytes,
        )
        INGESTION_EVENTS_REJECTED_TOTAL.labels(error_type=error_type).inc()
        logger.warning(
            "Event rejected and routed to DLQ [%s]: %s",
            error_type,
            error_message,
            extra={"error_type": error_type, "dlq_topic": self.dlq_topic},
        )

    def process_event(self, raw_event: Dict[str, Any]) -> bool:
        """Executes the full ingestion pipeline on a single incoming event record.

        Returns True if successfully published to the main topic, False if routed to DLQ.
        """
        try:
            normalized = self.normalize(raw_event)
            event_id = normalized["event_id"]
            set_correlation_id(event_id)

            # Validate against Avro schema
            if not self.validate_avro(normalized):
                self.publish_to_dlq(
                    raw_event=raw_event,
                    error_message="Avro schema validation failed for event envelope",
                    error_type="AVRO_VALIDATION_ERROR",
                )
                return False

            # Convert to domain model and publish
            event = PulseStreamEvent.from_avro_dict(normalized)
            produce_event(self.producer, self.events_topic, event)

            # Record metrics
            INGESTION_EVENTS_TOTAL.labels(
                source=event.source.value,
                event_type=event.event_type.value,
            ).inc()

            logger.debug(
                "Ingested event: id=%s source=%s type=%s community=%s",
                event.event_id,
                event.source.value,
                event.event_type.value,
                event.community_id,
            )
            return True

        except Exception as exc:
            error_msg = f"Unexpected pipeline exception: {str(exc)}"
            self.publish_to_dlq(
                raw_event=raw_event,
                error_message=error_msg,
                error_type="PROCESSING_ERROR",
            )
            return False

    def flush(self, timeout: float = 10.0) -> int:
        """Flushes buffered producer messages to Kafka."""
        return self.producer.flush(timeout)
