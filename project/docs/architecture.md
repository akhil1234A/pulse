# PulseStream Architecture Documentation

This document provides a deep architectural overview of **PulseStream**, an enterprise-grade, distributed real-time streaming platform designed to ingest, process, aggregate, and monitor community engagement events at high velocity with sub-second latency and zero data loss.

---

## 1. System Overview

PulseStream processes real-time event streams from Wikipedia SSE (live edits, creations, categorizations) and synthetic event simulators. It routes events through a high-throughput **Apache Kafka** cluster, processes them using decoupled consumer groups running **at-least-once** with idempotent storage, aggregates rolling window analytics in **Redis**, archives immutable events into time-partitioned **PostgreSQL**, and streams live updates via **FastAPI WebSockets** to an interactive dashboard.

```
+-----------------------------------------------------------------------------------------------+
|                                      PULSESTREAM ARCHITECTURE                                 |
+-----------------------------------------------------------------------------------------------+
|                                                                                               |
|  [Wikipedia SSE]     [Synthetic Simulator]                                                    |
|         \                    /                                                                |
|          v                  v                                                                 |
|   +---------------------------------+                                                         |
|   |    Ingestion Pipeline Engine    |  (Kafka Producer, Avro Serializer, Concurrency Limiter) |
|   +---------------------------------+                                                         |
|                    |                                                                          |
|                    v (Partition Key: community_id)                                            |
|   +-------------------------------------------------------------+                             |
|   |                 Apache Kafka (KRaft Mode)                   |                             |
|   |  - Topic: pulsestream.events (3 Partitions, LZ4, 7d)        |                             |
|   |  - Topic: pulsestream.dlq    (1 Partition, Retention 14d)   |                             |
|   +-------------------------------------------------------------+                             |
|              |                                           |                                    |
|              v (Group: ps-aggregation)                   v (Group: ps-anomaly-detector)       |
|   +-----------------------------+             +-----------------------------+                 |
|   |     Aggregation Worker      |             |   Anomaly Detector Worker   |                 |
|   |  - Real-time Redis Counters |             |  - User Flood Windowing     |                 |
|   |  - HyperLogLog Active Users |             |  - Community Rate Spikes    |                 |
|   |  - Batch Staging to Postgres|             |  - Lag / Deserialization DLQ|                 |
|   +-----------------------------+             +-----------------------------+                 |
|              |                \                              |                                |
|              v                 v                             v                                |
|       [( Redis 7 )]     [( PostgreSQL 16 )]           [( Redis / PG )]                        |
|       - Sliding windows - Time-partitioned events     - Active anomalies                      |
|       - Fast deduplicat - Dead-letter events log      - Alert audit trail                     |
|              |                         |                     |                                |
|              +-------------------------+---------------------+                                |
|                                        |                                                      |
|                                        v                                                      |
|                        +-------------------------------+                                      |
|                        |      FastAPI REST & WS API    |                                      |
|                        +-------------------------------+                                      |
|                           /          |             \                                          |
|                          v           v              v                                         |
|                 [HTTP Endpoints] [WebSocket WS] [Prometheus /metrics]                         |
|                        |             |              |                                         |
|                        v             v              v                                         |
|                  [Web Clients] [Dashboard UI]  [Grafana Dashboards]                           |
+-----------------------------------------------------------------------------------------------+
```

---

## 2. End-to-End Data Flow

The diagram below illustrates the exact sequence and boundary crossings for an event traveling from ingestion to the end-user dashboard.

```mermaid
sequenceDiagram
    autonumber
    participant Src as Event Source (Wiki / Sim)
    participant Ingest as Ingestion Pipeline
    participant Kafka as Kafka Broker (KRaft)
    participant AggWorker as Aggregation Worker
    participant AnomWorker as Anomaly Worker
    participant Redis as Redis Cache
    participant PG as PostgreSQL (Events Table)
    participant API as FastAPI Server
    participant UI as Web Dashboard

    Src->>Ingest: Stream raw event payload
    Ingest->>Ingest: Validate, normalize, assign UUID & correlation_id
    Ingest->>Ingest: Serialize to Apache Avro binary schema
    Ingest->>Kafka: Produce to 'pulsestream.events' (Key: community_id)
    
    par Consumer Group: ps-aggregation
        Kafka->>AggWorker: Consume batch (max 100 msgs)
        AggWorker->>Redis: Check & Set Deduplication key (TTL 3600s)
        alt Not Duplicate
            AggWorker->>Redis: INCR metrics (1s, 1m, 1h, source, type)
            AggWorker->>Redis: PFADD active_users (HyperLogLog)
            AggWorker->>Redis: ZINCRBY trending_communities
            AggWorker->>AggWorker: Buffer into PostgreSQL batch buffer
            opt Buffer size >= 500 OR Flush timer >= 2.0s
                AggWorker->>PG: COPY to temp_events -> ON CONFLICT DO NOTHING
            end
        end
        AggWorker->>Kafka: Synchronous Commit (Manual Offset)
    and Consumer Group: ps-anomaly-detector
        Kafka->>AnomWorker: Consume batch (max 100 msgs)
        AnomWorker->>Redis: Query sliding window zsets (User Flood & Community Spike)
        opt Threshold Exceeded
            AnomWorker->>PG: Insert into anomalies table
            AnomWorker->>Redis: Publish alert to 'pulsestream:anomalies'
        end
        AnomWorker->>Kafka: Synchronous Commit (Manual Offset)
    end

    loop Periodic Broadcast (Every 1.0s)
        API->>Redis: Fetch live aggregate metrics & trending
        API->>UI: Broadcast JSON payload via WebSocket (/ws/live)
    end
```

---

## 3. Consumer Group Topology & Scaling Model

PulseStream separates operational concerns by establishing independent Kafka Consumer Groups. Each consumer group maintains its own offset checkpoint in `__consumer_offsets`, ensuring that slow analytical queries or database batching never block real-time anomaly detection.

```mermaid
graph TD
    subgraph KafkaCluster["Kafka Cluster (pulsestream.events)"]
        P0["Partition 0<br/>Hash(community_id) % 3 == 0"]
        P1["Partition 1<br/>Hash(community_id) % 3 == 1"]
        P2["Partition 2<br/>Hash(community_id) % 3 == 2"]
    end

    subgraph AggGroup["Consumer Group: ps-aggregation"]
        AG1["Worker Instance 1<br/>Assigned: Partition 0, 1"]
        AG2["Worker Instance 2<br/>Assigned: Partition 2"]
    end

    subgraph AnomGroup["Consumer Group: ps-anomaly-detector"]
        AN1["Worker Instance 1<br/>Assigned: Partition 0"]
        AN2["Worker Instance 2<br/>Assigned: Partition 1, 2"]
    end

    P0 -->|Messages| AG1
    P1 -->|Messages| AG1
    P2 -->|Messages| AG2

    P0 -->|Messages| AN1
    P1 -->|Messages| AN2
    P2 -->|Messages| AN2

    style KafkaCluster fill:#f5f7fa,stroke:#333,stroke-width:2px;
    style AggGroup fill:#e8f4fd,stroke:#2b6cb0,stroke-width:2px;
    style AnomGroup fill:#fef3c7,stroke:#b45309,stroke-width:2px;
```

### Partitioning & Affinity
1. **Partition Key**: All events are produced with `key = event.community_id`.
2. **Per-Community Total Ordering**: Because all events for a given `community_id` hash to the exact same partition, per-community event order is strictly preserved.
3. **Independent Horizontal Scaling**:
   - The aggregation workers can scale up to the total number of topic partitions (3 in default local deployment, dynamically expandable in production).
   - Anomaly detector instances scale independently without impacting aggregation worker throughput.

---

## 4. Failure Handling, Circuit Breakers & DLQ Flow

Distributed stream processing systems must survive network partitions, downstream database exhaustion, and malformed poison pills without silent data loss.

```mermaid
stateDiagram-v2
    [*] --> Closed : System Healthy

    state Closed {
        [*] --> NormalOperation
        NormalOperation --> NormalOperation : Success (Failures = 0)
        NormalOperation --> FailureCount : Downstream Exception
        FailureCount --> NormalOperation : Failure Count < Threshold (5)
    }

    Closed --> Open : Consecutive Failures >= 5
    
    state Open {
        [*] --> RejectAndFallback
        RejectAndFallback --> CoolOffWait : Buffer events locally / wait recovery_time (30s)
    }

    Open --> HalfOpen : 30 seconds elapsed

    state HalfOpen {
        [*] --> TestProbe
        TestProbe --> ProbeSuccess : Canary operation succeeds
        TestProbe --> ProbeFail : Canary operation fails
    }

    HalfOpen --> Closed : ProbeSuccess (System recovered)
    HalfOpen --> Open : ProbeFail (Re-open circuit for 30s)

    state DeadLetterQueue {
        PoisonPill --> DeserializationError : Malformed Avro bytes
        DownstreamExhaustion --> RetryLimitExceeded : Retries > MaxRetries
        DeserializationError --> DLQTopic : Produce to pulsestream.dlq
        RetryLimitExceeded --> DLQTopic : Produce to pulsestream.dlq
        DLQTopic --> DLQTable : Persist to dead_letter_events (PG)
    }
```

### Fault-Tolerance Guarantees
1. **At-Least-Once Delivery**:
   - `enable.auto.commit = false` across all consumers. Offsets are only committed to Kafka *after* the event batch has been processed and stored in Redis and the local buffer.
2. **Idempotent Ingestion**:
   - **Redis Dedup**: Every event ID is recorded via `SETNX ps:dedup:{event_id} 1 EX 3600`. Duplicate arrivals from consumer rebalances or producer retries are immediately discarded.
   - **PostgreSQL Merge**: Bulk inserts execute via a staging table: `INSERT INTO events (...) SELECT ... FROM temp_events ON CONFLICT (event_id, timestamp) DO NOTHING`.
3. **Circuit Breakers**:
   - PostgreSQL inserts are protected by `CircuitBreaker(failure_threshold=5, recovery_timeout=30.0)`.
   - When PostgreSQL is down or lagging, events remain safely buffered in memory or backed up on Kafka's durable partition log.
4. **Dead Letter Queue (DLQ)**:
   - Poison pill messages (e.g. unparseable bytes, schema version mismatches) cannot crash workers. They are intercepted, tagged with stack traces, and routed to `pulsestream.dlq` and the `dead_letter_events` PostgreSQL table for operational triage.

---

## 5. Storage Architecture

### PostgreSQL Partitioning Strategy
The `events` table uses **declarative range partitioning** by `timestamp`.

```sql
CREATE TABLE events (
    event_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    source VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    user_id VARCHAR(100),
    community_id VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_version VARCHAR(10) NOT NULL DEFAULT '1.0.0',
    CONSTRAINT pk_events PRIMARY KEY (event_id, timestamp)
) PARTITION BY RANGE (timestamp);
```

- **Daily Partitions**: Partitions are provisioned daily (`events_y2026m09d17`, etc.).
- **Partition Pruning**: Time-bounded range queries (`WHERE timestamp >= NOW() - INTERVAL '1 hour'`) only touch current partitions, eliminating full-table scans.
- **High-Speed Ingestion**: Workers use `asyncpg` COPY protocol to write records directly to a temporary table in memory, executing single-transaction merge into partitioned storage.

### Redis Aggregations & Key Space
| Key Pattern | Redis Type | Purpose | TTL |
| :--- | :--- | :--- | :--- |
| `ps:dedup:{event_id}` | String | Idempotency deduplication | 3,600s (1 hr) |
| `ps:agg:events_per_sec:{timestamp}` | Counter | Real-time events per second throughput | 300s (5 min) |
| `ps:agg:events_today:{date}` | Counter | Total events processed today | 172,800s (48 hrs) |
| `ps:agg:type:{event_type}` | Counter | Cumulative breakdown by event type | None (Persistent) |
| `ps:agg:source:{source}` | Counter | Cumulative breakdown by ingestion source | None (Persistent) |
| `ps:agg:active_users:{window}` | HyperLogLog | Cardinality estimation of distinct users | 172,800s (48 hrs) |
| `ps:agg:community:{community_id}` | Counter | Per-community event count | None (Persistent) |
| `ps:agg:trending_communities` | Sorted Set | Top communities by current velocity | None (Persistent) |
| `ps:anom:user:{user_id}` | Sorted Set | Sliding window for user flood detection | 600s (10 min) |
| `ps:anom:community:{comm_id}`| Sorted Set | Sliding window for spike rate detection | 3,600s (1 hr) |

---

## 6. Observability & Telemetry

PulseStream includes production-grade observability baked into every component:

1. **Prometheus Metrics**:
   - `pulsestream_events_ingested_total` (counter by source, type, status)
   - `pulsestream_events_processed_total` (counter by worker, status)
   - `pulsestream_processing_duration_seconds` (histogram with p50, p95, p99 latency buckets)
   - `pulsestream_batch_size` (histogram of consumer batch sizes)
   - `pulsestream_db_insert_latency_seconds` (histogram of PostgreSQL batch COPY durations)
   - `pulsestream_anomalies_detected_total` (counter by rule and severity)
   - `pulsestream_dlq_events_total` (counter of malformed events routed to DLQ)
2. **Health Endpoints**:
   - `/health` on API (port 8000), Aggregation Worker (port 9101), and Anomaly Worker (port 9102).
   - Verifies active connectivity to Kafka Admin client, Redis ping, and PostgreSQL query pool.
3. **Structured Logging**:
   - Standardized JSON logs across all services containing `timestamp`, `level`, `logger`, `correlation_id`, and `service`.
