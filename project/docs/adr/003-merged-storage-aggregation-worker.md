# ADR 003: Merged Storage and Aggregation Worker Process

## Status
Accepted

## Context
The original architecture plan contemplated two separate downstream consumer groups:
1. `ps-storage`: A worker group solely responsible for reading from Kafka and batching event rows into the partitioned PostgreSQL database.
2. `ps-aggregation`: A worker group reading from Kafka and computing rolling-window counts, trending communities, and active user metrics in Redis.

During system benchmarking and resource evaluation, running two independent consumer groups for storage and aggregation introduced several disadvantages:
- **Redundant Kafka Reads**: Every message in `pulsestream.events` had to be read, decompressed, and deserialized twice from Kafka (once for storage, once for Redis).
- **Network & I/O Overhead**: Double the consumer connections and partition rebalances on the Kafka cluster.
- **Resource Footprint**: Running two distinct Python worker daemon processes per partition doubled Python runtime memory consumption (~150MB each).
- **Coordination Lag**: Storage and aggregation could diverge in lag, causing the API to report aggregated stats for events that were not yet queryable in PostgreSQL.

## Decision
We decided to merge the PostgreSQL batch storage and Redis real-time aggregation logic into a single **AggregationWorker** process operating under consumer group `ps-aggregation`.

### How It Works:
Within `AggregationWorker.process_batch()`:
1. **Deduplication**: Runs Redis check-and-set dedup for each incoming event.
2. **In-Memory Redis Pipeline**: Executes all metric updates (`INCR`, `PFADD`, `ZINCRBY`) across the entire batch in a single pipelined Redis network call.
3. **In-Memory Staging Buffer**: Appends valid events to a thread-safe local buffer (`self._pg_buffer`).
4. **Asynchronous Batched Flush**:
   - The buffer is flushed to PostgreSQL using `COPY` staging whenever the buffer reaches 500 events OR every 2.0 seconds (whichever happens first).
   - If PostgreSQL is slow or unavailable, a Circuit Breaker prevents worker crashes, and events remain queued in the buffer up to memory limits.
5. **Single Offset Commit**: The Kafka offset is committed once both Redis operations and in-memory buffering complete.

## Consequences

### Positive
- **50% Reduction in Kafka Read Overhead**: Messages are decompressed and parsed once per partition.
- **Lower Memory Usage**: Halves the number of worker containers/processes required.
- **Tight Consistency**: Events are reflected in Redis metrics at nearly the same millisecond they enter the database buffer.
- **Simpler Deployment**: One fewer service to orchestrate, monitor, and scale.

### Negative / Trade-offs
- **Coupled Failure Domains**: If PostgreSQL experiences severe latency and the buffer fills up, consumer consumption may pause to protect memory, temporarily delaying Redis real-time metrics. (Mitigated by the Circuit Breaker which sheds DB persistence during outages while keeping Redis real-time stats flowing).
