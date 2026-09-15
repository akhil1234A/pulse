"""Prometheus metrics definitions and helper utilities."""

import logging
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
    start_http_server,
)

logger = logging.getLogger(__name__)

# Ingestion metrics
INGESTION_EVENTS_TOTAL = Counter(
    "ingestion_events_total",
    "Total number of events ingested",
    labelnames=["source", "event_type"],
)

INGESTION_EVENTS_REJECTED_TOTAL = Counter(
    "ingestion_events_rejected_total",
    "Total number of rejected events sent to DLQ",
    labelnames=["error_type"],
)

# Worker metrics
WORKER_EVENTS_PROCESSED_TOTAL = Counter(
    "worker_events_processed_total",
    "Total number of events processed by workers",
    labelnames=["worker_type"],
)

WORKER_PROCESSING_LATENCY_SECONDS = Histogram(
    "worker_processing_latency_seconds",
    "Worker batch processing latency in seconds",
    labelnames=["worker_type"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

WORKER_BATCH_SIZE = Histogram(
    "worker_batch_size",
    "Number of events per polled batch",
    labelnames=["worker_type"],
    buckets=(1, 10, 50, 100, 250, 500, 1000),
)

# Storage metrics
STORAGE_BATCH_INSERT_LATENCY_SECONDS = Histogram(
    "storage_batch_insert_latency_seconds",
    "PostgreSQL COPY batch insert latency in seconds",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

STORAGE_ROWS_INSERTED_TOTAL = Counter(
    "storage_rows_inserted_total",
    "Total rows successfully inserted into durable storage",
)

# Kafka metrics
KAFKA_CONSUMER_LAG = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer group lag per partition",
    labelnames=["consumer_group", "partition"],
)

# Redis metrics
REDIS_DEDUP_HITS_TOTAL = Counter(
    "redis_dedup_hits_total",
    "Total number of duplicate events detected and dropped",
)

# Anomaly detection metrics
ANOMALY_DETECTED_TOTAL = Counter(
    "anomaly_detected_total",
    "Total anomalies detected by rule engine",
    labelnames=["anomaly_type", "severity"],
)

# API metrics
API_REQUEST_LATENCY_SECONDS = Histogram(
    "api_request_latency_seconds",
    "REST API endpoint request latency in seconds",
    labelnames=["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

API_WEBSOCKET_CONNECTIONS = Gauge(
    "api_websocket_connections",
    "Current active WebSocket client connections",
)

# Circuit Breaker state (0 = CLOSED, 1 = HALF_OPEN, 2 = OPEN)
CIRCUIT_BREAKER_STATE = Gauge(
    "circuit_breaker_state",
    "Current state of circuit breaker (0=Closed, 1=Half-Open, 2=Open)",
    labelnames=["component"],
)


def start_metrics_exporter(port: int = 9100) -> None:
    """Starts the Prometheus HTTP metrics server on specified port."""
    try:
        start_http_server(port)
        logger.info("Prometheus metrics server started on port %d", port)
    except Exception as e:
        logger.warning("Could not start Prometheus metrics exporter on port %d: %s", port, e)
