# PulseStream — Product Requirements Document

## 1. Overview

### 1.1 What is PulseStream?

PulseStream is a **real-time data platform** that ingests continuous streams of community events from real and synthetic data sources, processes them through distributed workers, and produces real-time analytics and anomaly detection — while handling the engineering challenges of throughput, failures, and resilience.

### 1.2 Why Build This?

Demonstrate mastery of the engineering problems behind platforms like Reddit, Discord, and GitHub:

- High-throughput event ingestion and processing
- Stream processing with Kafka
- Real-time aggregation and anomaly detection
- Failure recovery and resilience
- Observability at scale

### 1.3 Hardware Reality

This project is designed to run on a **single machine with 16GB RAM**. The architecture is scoped accordingly — fewer replicas, lighter infrastructure — but the **engineering patterns** (idempotency, backpressure, circuit breakers, chaos testing) are identical to what runs in production at scale.

---

## 2. Data Sources

### 2.1 Real Data Source

| Source | What It Provides | Why |
|---|---|---|
| **Wikipedia Recent Changes** (`stream.wikimedia.org`) | Page edits, new pages, log entries via SSE | True real-time stream. No rate limits. Continuous, high-volume, global. No API key needed |

### 2.2 Synthetic Data Source

| Source | What It Provides | Why |
|---|---|---|
| **Event Simulator** | Configurable synthetic events at controlled rates | Load testing, burst simulation, chaos injection (duplicates, malformed events, out-of-order) |

> **Why no GitHub Events API?** Rate-limited to 60 req/hr unauthenticated (5000 with token). At ~30 events per response, that's only ~0.5-2.5 events/sec — not useful for throughput testing. Wikipedia's SSE stream provides unlimited real-time volume with zero hassle.

### 2.3 Unified Event Schema

All sources normalized into a common envelope. Schema enforcement via **Avro**.

```json
{
  "event_id": "uuid-v4",
  "event_type": "POST_CREATED | COMMENT_CREATED | REACTION_ADDED | USER_JOINED",
  "source": "wikipedia | simulator",
  "timestamp": "2026-09-14T22:00:00Z",
  "user_id": "string",
  "community_id": "string | null",
  "payload": {},
  "metadata": {
    "ingested_at": "2026-09-14T22:00:00.123Z",
    "schema_version": "1.0"
  }
}
```

---

## 3. Functional Requirements

### FR-1: Ingestion

| ID | Requirement |
|---|---|
| FR-1.1 | Ingest events from Wikipedia Recent Changes via SSE stream |
| FR-1.2 | Ingest events from configurable event simulator (steady, burst, chaos modes) |
| FR-1.3 | Validate all events against Avro schema |
| FR-1.4 | Reject malformed events to dead-letter topic with error metadata |
| FR-1.5 | Assign globally unique `event_id` for deduplication |

### FR-2: Processing

| ID | Requirement |
|---|---|
| FR-2.1 | **Aggregation worker**: real-time counters — events/sec, active users, events by type, trending communities |
| FR-2.2 | **Anomaly worker**: detect user floods (>50 events/60s), community spikes (>10x average), duplicate storms (>5% dupes) |
| FR-2.3 | Deduplicate events via Redis (event_id lookup) |
| FR-2.4 | Exponential backoff with jitter on transient failures |
| FR-2.5 | Dead-letter queue after max retries |
| FR-2.6 | Batch insert to PostgreSQL (COPY protocol) |

### FR-3: API

| ID | Requirement |
|---|---|
| FR-3.1 | REST API (FastAPI) for events, aggregations, anomalies |
| FR-3.2 | WebSocket for real-time dashboard updates |
| FR-3.3 | Pagination and filtering (type, source, community, time range) |
| FR-3.4 | Health check endpoint |

### FR-4: Dashboard

| ID | Requirement |
|---|---|
| FR-4.1 | Real-time metrics: events/sec, active users, total events |
| FR-4.2 | Trending communities list |
| FR-4.3 | Anomaly alerts panel |
| FR-4.4 | Live event stream (last 50 events) |
| FR-4.5 | Event search with filters |

### FR-5: Observability

| ID | Requirement |
|---|---|
| FR-5.1 | Prometheus metrics from all services |
| FR-5.2 | Grafana dashboard for pipeline health |
| FR-5.3 | Structured JSON logging with correlation IDs |

### FR-6: Chaos Testing

| ID | Requirement |
|---|---|
| FR-6.1 | Consumer crash recovery test |
| FR-6.2 | Duplicate event injection and dedup verification |
| FR-6.3 | Malformed event injection and DLQ verification |
| FR-6.4 | Redis unavailability and graceful degradation test |

---

## 4. Non-Functional Requirements

| Category | Target |
|---|---|
| **Throughput** | 1,000–5,000 events/sec sustained |
| **Latency** | P50 < 1s, P95 < 3s (ingestion to dashboard) |
| **Durability** | Zero event loss under normal operation |
| **Recovery** | Auto-recover from single component failure < 60s |
| **Idempotency** | 100% deduplication accuracy |
| **RAM footprint** | < 6GB total (all containers) |

---

## 5. Scope Boundaries

### In Scope

- Wikipedia + Simulator ingestion
- Kafka streaming (single broker, KRaft mode)
- Aggregation + anomaly detection workers
- PostgreSQL with time-based partitioning
- Redis for real-time state and dedup
- FastAPI REST + WebSocket API
- Real-time dashboard
- Prometheus + Grafana (1 dashboard)
- Docker Compose deployment
- 4 chaos tests
- Benchmark report (3 runs)

### Out of Scope

- Multi-broker Kafka cluster
- GitHub Events API
- Kubernetes
- OpenSearch / Elasticsearch
- S3 / MinIO
- pgbouncer (asyncpg connection pool is sufficient)
- Nginx (FastAPI serves directly)
- CI/CD pipeline
- User authentication
- ML-based anomaly detection

---

## 6. Success Criteria

1. ✅ Wikipedia events flow through the full pipeline to the dashboard in real time
2. ✅ Simulator sustains 5K events/sec for 5 minutes
3. ✅ Anomaly detection flags injected bot behavior
4. ✅ Killing a worker causes rebalance with zero event loss
5. ✅ Duplicates are deduplicated with 100% accuracy
6. ✅ Benchmark report with P50/P95/P99 latency
7. ✅ Grafana dashboard shows system health
8. ✅ Total RAM footprint < 6GB
