# ADR 002: Manual Offset Commits with Multi-Layer Idempotent Processing

## Status
Accepted

## Context
In a distributed streaming architecture, failure modes such as consumer crashes, transient network timeouts, and container rebalancing can lead to message re-delivery. Stream consumers must guarantee that no data is lost (**at-least-once**), while preventing double-counting of metrics or duplicate database records.

Two common architectural choices exist:
1. **At-Most-Once Delivery**: Auto-commit offsets as soon as messages are polled, before processing. Risk: Messages are lost permanently if the worker crashes during processing.
2. **Exactly-Once Semantics (EOS)**: Kafka Transactions (`transactional.id`, 2-phase commit). Trade-off: High computational overhead, complexity, latency penalties, and limited cross-system transaction support (Kafka to Redis/PostgreSQL).
3. **At-Least-Once Delivery with Idempotent Handlers**: Disable auto-commit, process events through deduplication layers, persist records, and manually commit offsets.

## Decision
We decided on **At-Least-Once delivery via synchronous manual offset commits combined with multi-layer idempotency** across storage systems.

### Implementation Architecture:
1. **Disabled Auto-Commit**:
   ```python
   "enable.auto.commit": False
   ```
2. **Synchronous Manual Commit After Processing**:
   Offsets are only committed via `consumer.commit(asynchronous=False)` after:
   - All Redis increments succeed.
   - All events are safely enqueued in the PostgreSQL batch buffer or flushed.
3. **Redis Fast Deduplication Layer**:
   - Every event has a deterministic UUID (`event_id`).
   - Before processing, the worker checks and sets `ps:dedup:{event_id}` with a 1-hour TTL using atomic `SET key value NX EX 3600`.
   - If the key exists, the event is immediately flagged as duplicate and skipped from metric increments.
4. **PostgreSQL Idempotent Insertion**:
   - The primary key on the partitioned events table is `(event_id, timestamp)`.
   - Batch flushes stage records through a temporary table using the high-performance PostgreSQL `COPY` protocol, followed by:
     ```sql
     INSERT INTO events SELECT * FROM temp_events
     ON CONFLICT (event_id, timestamp) DO NOTHING;
     ```

## Consequences

### Positive
- **Zero Data Loss**: If a worker node crashes mid-batch, Kafka redelivers the uncommitted messages to the next consumer.
- **Strict Analytical Accuracy**: Redis metric counters and trending zsets are not inflated during rebalancing or retries because duplicates are rejected at step 3.
- **Database Consistency**: Re-delivered records are silently discarded by PostgreSQL's `ON CONFLICT DO NOTHING` without throwing exceptions or corrupting tables.
- **Simplicity**: Far simpler and more robust than distributed 2-phase commit transactions spanning Kafka, Redis, and PostgreSQL.

### Negative / Trade-offs
- **Redis Memory Overhead**: Redis must store deduplication keys for 1 hour. At 1,000 events/sec, 3.6 million keys require approximately 180MB of RAM.
- **Commit Latency**: Synchronous Kafka commit adds a ~1-3ms network round-trip per batch (mitigated by batching 100 messages per poll).
