"""Unit tests for the IngestionPipeline."""

import json
from unittest.mock import MagicMock
import pytest

from src.common.models import EventSource, EventType
from src.ingestion.pipeline import IngestionPipeline


def test_pipeline_normalize():
    mock_producer = MagicMock()
    pipeline = IngestionPipeline(producer=mock_producer)

    raw = {
        "event_type": "POST_CREATED",
        "source": "SIMULATOR",
        "timestamp": 1700000000000,
        "user_id": "usr_test",
        "payload": {"data": 123},
    }

    normalized = pipeline.normalize(raw)
    assert "event_id" in normalized
    assert "ingested_at" in normalized
    assert normalized["schema_version"] == "1.0"
    assert isinstance(normalized["payload"], str)


def test_pipeline_process_valid_event():
    mock_producer = MagicMock()
    pipeline = IngestionPipeline(
        producer=mock_producer,
        events_topic="pulsestream.events",
        dlq_topic="pulsestream.events.dlq",
    )

    raw = {
        "event_id": "c1f7607e-71cb-46a2-a0b4-93ec5ff9e69e",
        "event_type": EventType.POST_CREATED.value,
        "source": EventSource.SIMULATOR.value,
        "timestamp": 1700000000000,
        "user_id": "usr_001",
        "community_id": "technology",
        "payload": json.dumps({"headline": "New breakthroughs"}),
    }

    success = pipeline.process_event(raw)
    assert success is True

    # Producer was called for the main events topic
    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args.kwargs
    assert call_kwargs["topic"] == "pulsestream.events"
    assert call_kwargs["key"] == b"technology"
    assert isinstance(call_kwargs["value"], bytes)


def test_pipeline_process_invalid_event_routes_to_dlq():
    mock_producer = MagicMock()
    pipeline = IngestionPipeline(
        producer=mock_producer,
        events_topic="pulsestream.events",
        dlq_topic="pulsestream.events.dlq",
    )

    # Invalid: non-existent enum value for event_type
    invalid_raw = {
        "event_id": "c1f7607e-71cb-46a2-a0b4-93ec5ff9e69e",
        "event_type": "INVALID_UNKNOWN_TYPE",
        "source": "SIMULATOR",
        "timestamp": 1700000000000,
        "user_id": "usr_bad",
    }

    success = pipeline.process_event(invalid_raw)
    assert success is False

    # Producer should produce to DLQ topic
    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args.kwargs
    assert call_kwargs["topic"] == "pulsestream.events.dlq"

    dlq_data = json.loads(call_kwargs["value"].decode("utf-8"))
    assert dlq_data["error_type"] == "AVRO_VALIDATION_ERROR"
    assert "Avro schema validation failed" in dlq_data["error_message"]
    assert dlq_data["raw_event"]["user_id"] == "usr_bad"
