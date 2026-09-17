"""Integration test verifying IngestionPipeline end-to-end with live Kafka broker."""

import time

import pytest
from confluent_kafka import KafkaException

from src.common.config import get_settings
from src.common.kafka_utils import create_consumer
from src.common.models import PulseStreamEvent
from src.ingestion.connectors.simulator import EventSimulator
from src.ingestion.pipeline import IngestionPipeline


@pytest.mark.integration
def test_ingestion_produces_to_kafka_live():
    settings = get_settings()
    pipeline = IngestionPipeline()
    simulator = EventSimulator(mode="steady", rate=20)

    # Consume from Kafka with auto_offset_reset='latest' to read only newly produced events
    consumer = create_consumer(
        group_id=f"test-ingest-verify-{int(time.time())}",
        topics=[settings.kafka_events_topic],
        auto_offset_reset="latest",
    )
    # Warm up consumer and join group
    consumer.poll(timeout=2.0)

    # Process 10 events
    sent_ids = set()
    for _ in range(10):
        raw_evt = simulator.generate_single_event()
        success = pipeline.process_event(raw_evt)
        assert success is True
        sent_ids.add(raw_evt["event_id"])

    pipeline.flush(timeout=5.0)

    received_ids = set()
    start_time = time.time()

    while len(received_ids) < len(sent_ids) and (time.time() - start_time) < 15.0:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        event = PulseStreamEvent.from_avro_bytes(msg.value())
        if event.event_id in sent_ids:
            received_ids.add(event.event_id)

    consumer.close()

    assert received_ids == sent_ids
