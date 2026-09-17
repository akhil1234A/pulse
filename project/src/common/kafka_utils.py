"""Kafka producer and consumer utilities based on confluent-kafka."""

import logging
from typing import Any, Callable, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, Message, Producer

from src.common.config import get_settings
from src.common.models import PulseStreamEvent

logger = logging.getLogger(__name__)


def get_partition_key(community_id: Optional[str], user_id: str) -> str:
    """Derives partition key based on community_id, falling back to user_id."""
    return community_id if community_id else user_id


def default_delivery_callback(err: Optional[KafkaError], msg: Message) -> None:
    """Default callback for Kafka producer delivery reports."""
    if err is not None:
        logger.error(
            "Message delivery failed",
            extra={"error": str(err), "topic": msg.topic(), "partition": msg.partition()},
        )
    else:
        logger.debug(
            "Message delivered successfully",
            extra={
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            },
        )


def get_producer_config(
    bootstrap_servers: Optional[str] = None,
    extra_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generates standard producer configuration matching the Technical Specification."""
    settings = get_settings()
    config: Dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers or settings.kafka_bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "batch.size": 32768,  # 32KB
        "linger.ms": 10,
        "retries": 5,
        "retry.backoff.ms": 100,
    }
    if extra_config:
        config.update(extra_config)
    return config


def create_producer(
    bootstrap_servers: Optional[str] = None,
    extra_config: Optional[Dict[str, Any]] = None,
) -> Producer:
    """Instantiates a confluent-kafka Producer."""
    config = get_producer_config(bootstrap_servers, extra_config)
    return Producer(config)


def get_consumer_config(
    group_id: str,
    bootstrap_servers: Optional[str] = None,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    extra_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generates standard consumer configuration matching the Technical Specification."""
    settings = get_settings()
    config: Dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers or settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": enable_auto_commit,
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 10000,
    }
    if extra_config:
        config.update(extra_config)
    return config


def create_consumer(
    group_id: str,
    topics: Optional[List[str]] = None,
    bootstrap_servers: Optional[str] = None,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    extra_config: Optional[Dict[str, Any]] = None,
) -> Consumer:
    """Instantiates and optionally subscribes a confluent-kafka Consumer."""
    config = get_consumer_config(
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        extra_config=extra_config,
    )
    consumer = Consumer(config)
    if topics:
        consumer.subscribe(topics)
    return consumer


def produce_event(
    producer: Producer,
    topic: str,
    event: PulseStreamEvent,
    on_delivery: Optional[Callable[[Optional[KafkaError], Message], None]] = None,
) -> None:
    """Serializes a PulseStreamEvent to Avro and produces it to Kafka with partition key."""
    key = get_partition_key(event.community_id, event.user_id)
    value_bytes = event.to_avro_bytes()
    callback = on_delivery or default_delivery_callback

    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=value_bytes,
        on_delivery=callback,
    )
