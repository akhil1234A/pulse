# PulseStream — Technical Specification

## 1. System Architecture

### 1.1 High-Level Architecture

```
       Data Sources
    ┌──────┼──────┐
    ↓              ↓
 Wikipedia     Simulator
 (SSE stream)  (burst/chaos)
    └──────┼──────┘
           ↓
   ┌──────────────┐
   │  Ingestion    │   Normalize → Validate (Avro) → Publish
   │  Service      │
   └──────┬───────┘
          ↓
   ┌──────────────┐
   │ Apache Kafka  │   1 broker (KRaft mode, no Zookeeper)
   │ 6 partitions  │   Topics: events, events-dlq
   └──────┬───────┘
      ┌───┴───┐
      ↓       ↓
 ┌─────────┐ ┌──────────┐
 │ Agg     │ │ Anomaly  │   2 consumer groups
 │ Worker  │ │ Worker   │   (each reads full stream)
 └────┬────┘ └────┬─────┘
      ↓           ↓
   ┌───────────────┐
   │     Redis      │   Counters, dedup, trending, anomaly state
   └───────┬───────┘
           ↓
   ┌───────────────┐
   │  PostgreSQL    │   Durable storage (partitioned by day)
   └───────┬───────┘
           ↓
   ┌───────────────┐
   │  FastAPI API   │   REST + WebSocket + static dashboard
   └───────┬───────┘
           ↓
   ┌───────────────┐
   │  Dashboard     │   HTML/CSS/JS (served by FastAPI)
   └───────────────┘

   ┌───────────────┐
   │  Prometheus    │──→ Grafana   (Observability)
   └───────────────┘
```

### 1.2 Service Inventory

| Service | Instances | RAM Limit | Role |
|---|---|---|---|
| `kafka` | 1 | 1 GB | Event streaming (KRaft, no Zookeeper) |
| `postgresql` | 1 | 512 MB | Durable event storage |
| `redis` | 1 | 256 MB | Real-time state, dedup, caching |
| `ingestion` | 1 | 256 MB | Wikipedia SSE + simulator → Kafka |
| `agg-worker` | 1 | 256 MB | Real-time aggregation |
| `anomaly-worker` | 1 | 256 MB | Anomaly detection |
| `api-server` | 1 | 256 MB | REST + WebSocket + serves dashboard |
| `prometheus` | 1 | 256 MB | Metrics collection |
| `grafana` | 1 | 256 MB | Metrics visualization |
| **Total** | **9** | **~3.3 GB** | |

> **Why 1 Kafka broker?** On 16GB RAM with Docker Desktop overhead (~2-4GB), running 3 brokers + Zookeeper would consume 5GB+ just for Kafka. A single KRaft broker handles 10K+ events/sec easily and demonstrates the same engineering patterns (partitioning, consumer groups, offset management). The architecture is designed so that scaling to 3 brokers is a config change, not a code change.

---

## 2. Data Models

### 2.1 Avro Schema — Event Envelope

```json
{
  "type": "record",
  "name": "PulseStreamEvent",
  "namespace": "com.pulsestream.events",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_type", "type": {
      "type": "enum",
      "name": "EventType",
      "symbols": ["POST_CREATED", "COMMENT_CREATED", "REACTION_ADDED", "USER_JOINED"]
    }},
    {"name": "source", "type": {
      "type": "enum",
      "name": "EventSource",
      "symbols": ["WIKIPEDIA", "SIMULATOR"]
    }},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "user_id", "type": "string"},
    {"name": "community_id", "type": ["null", "string"], "default": null},
    {"name": "payload", "type": "string"},
    {"name": "ingested_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "schema_version", "type": "string", "default": "1.0"}
  ]
}
```

### 2.2 PostgreSQL Schema

```sql
-- Events table (partitioned by day)
CREATE TABLE events (
    event_id        UUID PRIMARY KEY,
    event_type      VARCHAR(50) NOT NULL,
    source          VARCHAR(20) NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    user_id         VARCHAR(255) NOT NULL,
    community_id    VARCHAR(255),
    payload         JSONB NOT NULL DEFAULT '{}',
    ingested_at     TIMESTAMPTZ NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_version  VARCHAR(10) NOT NULL DEFAULT '1.0'
) PARTITION BY RANGE (timestamp);

-- Indexes
CREATE INDEX idx_events_type ON events (event_type);
CREATE INDEX idx_events_source ON events (source);
CREATE INDEX idx_events_community ON events (community_id) WHERE community_id IS NOT NULL;
CREATE INDEX idx_events_timestamp ON events (timestamp DESC);
CREATE INDEX idx_events_payload_gin ON events USING gin (payload jsonb_path_ops);

-- Anomalies table
CREATE TABLE anomalies (
    id              SERIAL PRIMARY KEY,
    anomaly_type    VARCHAR(50) NOT NULL,
    severity        VARCHAR(10) NOT NULL,
    description     TEXT NOT NULL,
    details         JSONB NOT NULL DEFAULT '{}',
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_ids       UUID[] DEFAULT '{}'
);

CREATE INDEX idx_anomalies_detected ON anomalies (detected_at DESC);

-- Dead letter events
CREATE TABLE dead_letter_events (
    id              SERIAL PRIMARY KEY,
    raw_event       JSONB NOT NULL,
    error_message   TEXT NOT NULL,
    error_type      VARCHAR(100) NOT NULL,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 2.3 Redis Key Schema

```text
# Counters
ps:count:events_today                     → STRING (INCR, TTL midnight)
ps:count:type:{event_type}                → STRING (INCR, TTL 24h)
ps:count:source:{source}                  → STRING (INCR, TTL 24h)
ps:count:community:{id}                   → STRING (INCR, TTL 24h)

# Rate tracking
ps:rate:events_per_sec                    → ZSET (timestamp → count, trim >60s)
ps:rate:active_users                      → HyperLogLog (reset every 5min)

# Trending
ps:trending:communities                   → ZSET (community → score, TTL 1h)

# Deduplication
ps:dedup:{event_id}                       → STRING ("1", TTL 1h)

# Anomaly detection
ps:anomaly:user:{user_id}                 → ZSET (timestamp → event_id, TTL 5min)
ps:anomaly:community:{id}                 → ZSET (timestamp → event_id, TTL 5min)

# Cache
ps:cache:{endpoint_hash}                  → STRING (JSON, TTL 30s)
```

---

## 3. Kafka Topology

### 3.1 Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `pulsestream.events` | 6 | 7 days | Main event stream |
| `pulsestream.events.dlq` | 3 | 30 days | Dead letter queue |

> **6 partitions** — enough to demonstrate consumer group parallelism (can run up to 6 consumers per group). On a single machine, 1 consumer per group is typical, but the partition count allows scaling up for benchmarking.

### 3.2 Partitioning Strategy

Events partitioned by `community_id` hash (or `user_id` if null). This guarantees ordering within a community.

### 3.3 Producer Config

```python
PRODUCER_CONFIG = {
    "bootstrap.servers": "kafka:9092",
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "lz4",
    "batch.size": 32768,            # 32KB
    "linger.ms": 10,
}
```

### 3.4 Consumer Config

```python
CONSUMER_CONFIG = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "<group-id>",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,     # Manual commit after processing
    "max.poll.records": 500,
    "session.timeout.ms": 30000,
    "heartbeat.interval.ms": 10000,
}
```

---

## 4. Service Specifications

### 4.1 Ingestion Service

**Connectors:**

| Connector | Method | Notes |
|---|---|---|
| Wikipedia Changes | SSE stream via `aiohttp` | Auto-reconnect with backoff |
| Event Simulator | In-process async generator | Modes: steady, burst, chaos, ramp |

**Pipeline:** `receive → normalize → validate (Avro) → assign event_id → publish to Kafka`

Failed validation → publish to `pulsestream.events.dlq` with error metadata.

**Simulator Chaos Modes:**
- `steady`: constant rate (configurable events/sec)
- `burst`: 10x spike for 30 seconds
- `chaos`: inject duplicates (5%), malformed events (2%), out-of-order timestamps
- `ramp`: gradually increase from min to max rate

### 4.2 Aggregation Worker

**Consumer group:** `ps-aggregation`

For each batch:
1. Dedup check (`SISMEMBER ps:dedup:{event_id}`)
2. Update counters (Redis pipeline for batched ops)
3. Update trending (`ZINCRBY`)
4. Track active users (`PFADD` HyperLogLog)
5. Batch insert to PostgreSQL (COPY protocol, flush every 1000 events or 5 seconds)
6. Mark as seen (`SETEX ps:dedup:{event_id} 3600 1`)
7. Commit Kafka offset

> **Why merge storage into aggregation?** Running a separate storage worker on 16GB RAM adds another consumer competing for resources. Since aggregation already processes every event, adding batch-insert logic here is simpler and more efficient. If scaling later, split it out.

### 4.3 Anomaly Detection Worker

**Consumer group:** `ps-anomaly`

**Detection Rules:**

| Rule | Trigger | Severity |
|---|---|---|
| User Flood | user_id > 50 events in 60s | HIGH |
| Community Spike | community rate > 10x 1-hour average | MEDIUM |
| Duplicate Storm | > 5% duplicates in batch | HIGH |

Anomalies written to PostgreSQL `anomalies` table and published to Redis for dashboard display.

### 4.4 API Server (FastAPI)

**REST Endpoints:**
```text
GET  /api/v1/health              → dependency health
GET  /api/v1/metrics/realtime    → counters from Redis
GET  /api/v1/metrics/trending    → trending from Redis
GET  /api/v1/events              → paginated events (PostgreSQL)
GET  /api/v1/events/{event_id}   → single event
GET  /api/v1/anomalies           → recent anomalies
WS   /api/v1/ws/dashboard        → push updates every 1s
```

Also serves `dashboard/` static files at `/`.

### 4.5 Dashboard

Vanilla HTML/CSS/JS. WebSocket connection for real-time updates. No build step, no framework.

---

## 5. Failure Handling

### 5.1 Retry Policy

```python
# Exponential backoff with jitter
delay = min(100ms * 2^attempt + random(0, 100ms), 30s)
max_retries = 5
```

### 5.2 Dead Letter Queue

```
Event fails → retry (up to 5x) → DLQ topic + dead_letter_events table
```

### 5.3 Circuit Breaker

Applied to PostgreSQL and Redis connections:

```
CLOSED (normal) → 5 consecutive failures → OPEN (stop calling, 30s)
    → HALF_OPEN (try 1 request) → success → CLOSED / failure → OPEN
```

### 5.4 Graceful Degradation

| Component Down | Behavior |
|---|---|
| Redis | Workers skip dedup/caching, process continues, dashboard shows stale data |
| PostgreSQL | Aggregation worker pauses batch flush, Kafka lag grows, no data loss |
| Consumer crashes | Kafka rebalances, uncommitted events reprocessed (idempotent) |

---

## 6. Observability

### 6.1 Key Prometheus Metrics

```python
# Ingestion
ingestion_events_total{source, event_type}         # Counter
ingestion_events_rejected_total{error_type}         # Counter

# Workers
worker_events_processed_total{worker_type}          # Counter
worker_processing_latency_seconds{worker_type}      # Histogram
worker_batch_size{worker_type}                      # Histogram

# Storage
storage_batch_insert_latency_seconds                # Histogram
storage_rows_inserted_total                         # Counter

# Kafka
kafka_consumer_lag{consumer_group, partition}       # Gauge

# Redis
redis_dedup_hits_total                              # Counter

# Anomaly
anomaly_detected_total{anomaly_type, severity}      # Counter

# API
api_request_latency_seconds{method, endpoint}       # Histogram
api_websocket_connections                           # Gauge

# Circuit Breaker
circuit_breaker_state{component}                    # Gauge
```

### 6.2 Grafana Dashboard

One unified dashboard with panels:
- Events/sec throughput (all sources)
- End-to-end latency (P50/P95/P99)
- Kafka consumer lag per group
- Error rate and DLQ events
- Redis ops/sec and memory
- PostgreSQL write latency
- Circuit breaker states
- Container CPU/memory

---

## 7. Deployment (Docker Compose)

### 7.1 Services

```yaml
services:
  kafka:           # KRaft mode (single broker, no Zookeeper)
  postgresql:      # Single node, init script for schema
  redis:           # maxmemory 256mb, allkeys-lru
  ingestion:       # Python, connects Wikipedia SSE + simulator
  agg-worker:      # Python, aggregation + storage
  anomaly-worker:  # Python, anomaly detection
  api-server:      # FastAPI, serves REST + WS + static files
  prometheus:      # Scrapes all services
  grafana:         # Pre-provisioned dashboard
```

### 7.2 Resource Limits

| Service | CPU | Memory |
|---|---|---|
| kafka | 1.0 | 1 GB |
| postgresql | 0.5 | 512 MB |
| redis | 0.25 | 256 MB |
| ingestion | 0.25 | 256 MB |
| agg-worker | 0.5 | 256 MB |
| anomaly-worker | 0.25 | 256 MB |
| api-server | 0.25 | 256 MB |
| prometheus | 0.25 | 256 MB |
| grafana | 0.25 | 256 MB |
| **Total** | **~3.5** | **~3.3 GB** |

Leaves ~10GB for Windows + Docker Desktop overhead on a 16GB machine.

### 7.3 Networking

All services on a single `pulsestream` bridge network. API server exposed on `localhost:8000`.

---

## 8. Performance Targets

| Phase | Target | Validation |
|---|---|---|
| Baseline | 1,000 events/sec, 5 min | Should pass easily |
| Moderate | 3,000 events/sec, 5 min | May need batch tuning |
| Target | 5,000 events/sec, 5 min | Stretch — PostgreSQL is likely bottleneck |

Measurements: throughput, P50/P95/P99 latency, Kafka lag, CPU/memory per container.

---

## 9. Key Technology Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Kafka mode | KRaft (single broker) | No Zookeeper = 1 fewer container, simpler, saves ~1GB RAM |
| Schema format | Avro | Schema evolution, compact, Kafka-native |
| Connection pooling | asyncpg pool (no pgbouncer) | Built-in pooling is sufficient for 1 PostgreSQL instance |
| Reverse proxy | None (FastAPI direct) | Nginx unnecessary for single API instance |
| Dashboard framework | Vanilla HTML/CSS/JS | No build step, served as static files by FastAPI |
| Active user counting | HyperLogLog | 12KB vs storing all user IDs |
| Kafka commits | Manual | Ensures no event loss on crash |
| Batch writes | PostgreSQL COPY | 10-50x faster than individual INSERTs |
