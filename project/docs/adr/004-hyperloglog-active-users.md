# ADR 004: Redis HyperLogLog for Real-Time Unique Active User Cardinality

## Status
Accepted

## Context
PulseStream tracks real-time platform engagement, with **active distinct users** over rolling windows (e.g., last 24 hours, daily active users) being a critical KPI displayed on the real-time dashboard.

We evaluated three potential techniques to compute distinct active users:
1. **PostgreSQL Distinct Query (`COUNT(DISTINCT user_id)`)**:
   - Querying distinct values over millions of rows in a partitioned table requires an index scan or hash aggregate.
   - At high event volumes (10M+ events/day), running this query on every dashboard refresh or polling interval spikes DB CPU to 100% and introduces several seconds of query latency.
2. **Redis Sets (`SADD` / `SCARD`)**:
   - Exact uniqueness count using Redis hash sets.
   - Memory consumption scales linearly with cardinality: storing 5 million UUIDs (36 bytes each + set overhead) requires ~300MB to 500MB of expensive Redis RAM per time window.
3. **Redis HyperLogLog (`PFADD` / `PFCOUNT`)**:
   - Probabilistic data structure designed specifically for cardinality estimation with a standard error of 0.81%.
   - Fixed memory footprint of maximum 12 KB per key regardless of whether 100 or 100 million distinct users are tracked.

## Decision
We decided to use **Redis HyperLogLog (`PFADD` and `PFCOUNT`)** for tracking active distinct users across rolling daily and hourly windows.

### Implementation:
- Key pattern: `ps:agg:active_users:{window_date}` (e.g., `ps:agg:active_users:2026-09-17`).
- Worker update: On every processed event, the aggregation worker issues `PFADD ps:agg:active_users:{window_date} {user_id}`.
- Dashboard / API query: `PFCOUNT ps:agg:active_users:{window_date}` executes in $O(1)$ time (< 1ms).
- Window expiration: Keys are assigned a 48-hour TTL to prevent unbounded memory growth.

## Consequences

### Positive
- **Constant $O(1)$ Memory Footprint**: Exactly ~12 KB per day window, meaning 30 days of unique active user tracking uses less than 400 KB of Redis memory.
- **Microsecond Query Latency**: `PFCOUNT` returns instantaneously without scanning disks or locking database tables.
- **Mergeable Across Windows**: Redis `PFMERGE` allows combining daily HyperLogLogs into weekly or monthly active user totals without rescanning raw events.

### Negative / Trade-offs
- **Probabilistic Approximation**: HyperLogLog has a theoretical standard error of ~0.81%. For an active user count of 100,000, the estimated count will typically fall between 99,190 and 100,810. For operational dashboards and trend monitoring, this variance is completely imperceptible and standard industry practice.
- **Cannot Retrieve User IDs**: `PFCOUNT` returns only the estimated count, not the list of specific user IDs (raw event lookup in PostgreSQL is used if specific user history is required).
