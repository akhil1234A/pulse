"""Unit tests for Prometheus metrics definitions."""

import pytest
from src.common.metrics import (
    ANOMALY_DETECTED_TOTAL,
    API_REQUEST_LATENCY_SECONDS,
    API_WEBSOCKET_CONNECTIONS,
    CIRCUIT_BREAKER_STATE,
    INGESTION_EVENTS_REJECTED_TOTAL,
    INGESTION_EVENTS_TOTAL,
    KAFKA_CONSUMER_LAG,
    REDIS_DEDUP_HITS_TOTAL,
    STORAGE_BATCH_INSERT_LATENCY_SECONDS,
    STORAGE_ROWS_INSERTED_TOTAL,
    WORKER_BATCH_SIZE,
    WORKER_EVENTS_PROCESSED_TOTAL,
    WORKER_PROCESSING_LATENCY_SECONDS,
)


def test_metrics_increment_and_observe():
    # Ingestion
    INGESTION_EVENTS_TOTAL.labels(source="WIKIPEDIA", event_type="POST_CREATED").inc()
    INGESTION_EVENTS_REJECTED_TOTAL.labels(error_type="AVRO_VALIDATION").inc()

    # Worker
    WORKER_EVENTS_PROCESSED_TOTAL.labels(worker_type="aggregation").inc()
    WORKER_PROCESSING_LATENCY_SECONDS.labels(worker_type="aggregation").observe(0.015)
    WORKER_BATCH_SIZE.labels(worker_type="aggregation").observe(100)

    # Storage
    STORAGE_ROWS_INSERTED_TOTAL.inc(100)
    STORAGE_BATCH_INSERT_LATENCY_SECONDS.observe(0.02)

    # Kafka
    KAFKA_CONSUMER_LAG.labels(consumer_group="ps-aggregation", partition="0").set(5)

    # Redis
    REDIS_DEDUP_HITS_TOTAL.inc()

    # Anomaly
    ANOMALY_DETECTED_TOTAL.labels(anomaly_type="USER_FLOOD", severity="HIGH").inc()

    # API
    API_REQUEST_LATENCY_SECONDS.labels(method="GET", endpoint="/api/v1/health").observe(0.003)
    API_WEBSOCKET_CONNECTIONS.set(4)

    # Circuit Breaker
    CIRCUIT_BREAKER_STATE.labels(component="redis").set(0)
    CIRCUIT_BREAKER_STATE.labels(component="postgres").set(2)
