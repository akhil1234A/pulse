"""Unit tests for Kafka utilities."""

from unittest.mock import MagicMock

from src.common.kafka_utils import (
    get_consumer_config,
    get_partition_key,
    get_producer_config,
    produce_event,
)
from src.common.models import EventSource, EventType, PulseStreamEvent


def test_get_partition_key():
    assert get_partition_key("community_1", "user_1") == "community_1"
    assert get_partition_key(None, "user_1") == "user_1"
    assert get_partition_key("", "user_2") == "user_2"


def test_producer_config():
    config = get_producer_config(bootstrap_servers="test-kafka:9092")
    assert config["bootstrap.servers"] == "test-kafka:9092"
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["batch.size"] == 32768
    assert config["linger.ms"] == 10


def test_consumer_config():
    config = get_consumer_config(
        group_id="ps-test-group",
        bootstrap_servers="test-kafka:9092",
    )
    assert config["bootstrap.servers"] == "test-kafka:9092"
    assert config["group.id"] == "ps-test-group"
    assert config["auto.offset.reset"] == "earliest"
    assert config["enable.auto.commit"] is False


def test_produce_event_mock():
    mock_producer = MagicMock()
    event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.WIKIPEDIA,
        user_id="user_mock",
        community_id="wiki_comm",
        payload="{}",
    )

    produce_event(mock_producer, "pulsestream.events", event)

    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args.kwargs
    assert call_kwargs["topic"] == "pulsestream.events"
    assert call_kwargs["key"] == b"wiki_comm"
    assert isinstance(call_kwargs["value"], bytes)
