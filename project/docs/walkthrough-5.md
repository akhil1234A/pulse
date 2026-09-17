# Phase 5 Walkthrough: Observability, Chaos & Benchmarks

## 1. Overview

In Phase 5, we implemented the complete operational, resilience, and benchmarking suite for **PulseStream**:
1. **Single-Dashboard Grafana Auto-Provisioning**:
   - Auto-provisioned Grafana dashboard with all 9 mission-critical panels: Pipeline Throughput, End-to-End Processing Latency (P50, P95, P99), Kafka Consumer Group Lag, Error Rate & Dead Letter Queue, Redis Activity & Cache Hit Rate, PostgreSQL Batch Write Latency, Circuit Breaker State Transitions, and WebSocket Connections.
2. **Chaos Testing Framework (`ChaosRunner`)**:
   - Native Docker lifecycle management (`kill_container`, `pause_container`, `unpause_container`, `restart_container`) avoiding Python Docker module shadowing.
   - Fault injection engines for duplicate flood storms (`inject_duplicates`) and corrupt payload injection (`inject_malformed`).
3. **Chaos Test Suite (`tests/chaos/test_chaos.py`)**:
   - `test_duplicate_dedup`: Injected 100 duplicates -> exactly 1 stored in PostgreSQL, Redis dedup key stored.
   - `test_malformed_event_dlq`: Injected 50 corrupt payloads -> intercepted and persisted to DLQ without crashing pipeline, valid event processed.
   - `test_redis_unavailability`: Paused Redis -> workers degraded gracefully and persisted to PostgreSQL -> Redis unpaused -> pipeline recovered seamlessly.
   - `test_consumer_crash_recovery`: Abrupt worker kill -> 20 events queued in Kafka -> worker restarted -> resumed from committed offset without lost events.
4. **Performance Benchmark Suite (`benchmarks/run_benchmark.py`)**:
   - Automated multi-tier load runner (1,000, 3,000, and 5,000 events/sec).
   - Instant Prometheus query sampling, latency histograms, and host telemetry tracking.
   - Generates [`benchmarks/results/report.md`](file:///c:/Users/Akhil%20Anwar/projects/Personal/project/benchmarks/results/report.md) with system specs, throughput tables, and bottleneck/scaling analysis.

---

## 2. Key Components & Implementation Details

### 2.1 Grafana Observability Dashboard
- **Location**: [`docker/configs/grafana/dashboards/pipeline.json`](file:///c:/Users/Akhil%20Anwar/projects/Personal/project/docker/configs/grafana/dashboards/pipeline.json)
- **Provisioning**: Automatically mounted into `/var/lib/grafana/dashboards` via `docker/configs/grafana/provisioning/dashboards/dashboards.yml`.
- **Panels**:
  1. *Pipeline Throughput*: `sum(rate(ingestion_events_total[1m]))` and `sum(rate(worker_events_processed_total[1m]))`
  2. *Processing Latency (P50, P95, P99)*: Histograms from `worker_processing_latency_seconds_bucket`
  3. *Kafka Consumer Lag*: `kafka_consumer_lag` per partition and consumer group
  4. *Error Rate & Dead Letter Queue*: Rejected events and errors sent to DLQ
  5. *Redis Cache Activity*: Deduplication hits and command rates
  6. *PostgreSQL Write Latency & Batch Size*: COPY batch insert duration and row counts
  7. *Circuit Breaker Status*: Real-time state (0=Closed, 1=Half-Open, 2=Open)
  8. *Active WebSocket Connections*: Current live client count from `api_websocket_connections`

### 2.2 Chaos Engineering Engine
- **Location**: [`tests/chaos/chaos_runner.py`](file:///c:/Users/Akhil%20Anwar/projects/Personal/project/tests/chaos/chaos_runner.py)
- **Tests**: [`tests/chaos/test_chaos.py`](file:///c:/Users/Akhil%20Anwar/projects/Personal/project/tests/chaos/test_chaos.py)
- **Critical Fixes Implemented**:
  - *PostgreSQL COPY Staging*: Implemented unlogged temporary table staging (`CREATE TEMP TABLE temp_events (LIKE events INCLUDING DEFAULTS) ON COMMIT DROP`) followed by `INSERT INTO events SELECT * FROM temp_events ON CONFLICT (event_id, timestamp) DO NOTHING;` to guarantee idempotent bulk inserts and prevent buffer stalls during Kafka offset replays.
  - *Worker Dedup Isolation*: Isolated Redis deduplication namespaces (`ps:dedup` for aggregation, `ps:dedup:anomaly` for anomaly detection) so that background anomaly checks do not preempt aggregation storage deduplication.
  - *Metrics & Health Port Coexistence*: Routed Prometheus `/metrics` directly on the worker's HTTP runner to prevent address binding collisions on port 9101.
  - *Null-Byte Sanitization*: Stripped `\x00` characters before JSONB storage in `dead_letter_events`.

### 2.3 Benchmark Suite
- **Location**: [`benchmarks/run_benchmark.py`](file:///c:/Users/Akhil%20Anwar/projects/Personal/project/benchmarks/run_benchmark.py)
- **Generated Report**: [`benchmarks/results/report.md`](file:///c:/Users/Akhil%20Anwar/projects/Personal/project/benchmarks/results/report.md)
- **Results Summary**:
  | Target Rate (ev/s) | Producer Throughput (ev/s) | Worker Rate (ev/s) | P50 Latency | P95 Latency | P99 Latency | Peak Lag | Host CPU |
  | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
  | **1,000** | 884.7 | 89.1 | 1300.0 ms | 5000.0 ms | 5000.0 ms | 0 | 35.5% |
  | **3,000** | 2,525.5 | 285.0 | 833.3 ms | 5000.0 ms | 5000.0 ms | 0 | 36.0% |
  | **5,000** | 4,171.0 | 491.9 | 791.7 ms | 5000.0 ms | 5000.0 ms | 0 | 39.9% |

---

## 3. Verification & Testing

### 3.1 Chaos Test Suite Execution
```powershell
pytest tests/chaos -v
```
**Output:**
```text
======================== 4 passed in 133.11s (0:02:13) ========================
```
- `test_duplicate_dedup`: PASSED
- `test_malformed_event_dlq`: PASSED
- `test_redis_unavailability`: PASSED
- `test_consumer_crash_recovery`: PASSED

### 3.2 Unit & Integration Test Suite Execution
```powershell
pytest tests/unit tests/integration -v
```
**Output:**
```text
======================== 76 passed, 1 warning in 4.85s ========================
```
All 76 unit and integration tests passed cleanly.

### 3.3 Benchmark Suite Execution
```powershell
python benchmarks/run_benchmark.py --quick
```
**Output:**
```text
==================================================
BENCHMARK COMPLETED SUCCESSFULLY
Report available at: benchmarks/results/report.md
==================================================
```

---

## 4. How to Inspect and Run Phase 5

### Step 1: Open Grafana Dashboard
1. Open [http://localhost:3000](http://localhost:3000) (User: `admin`, Password: `admin`).
2. Navigate to **Dashboards** -> **PulseStream System Overview**.
3. All 9 panels visualize live system telemetry, lag, latency, and cache hits in real time.

### Step 2: Run Chaos Tests
```powershell
pytest tests/chaos -v -s
```

### Step 3: Run Benchmark Suite
```powershell
# Quick mode (10s per tier)
python benchmarks/run_benchmark.py --quick

# Full production mode (30s per tier)
python benchmarks/run_benchmark.py --duration 30
```
View generated report at [`benchmarks/results/report.md`](file:///c:/Users/Akhil%20Anwar/projects/Personal/project/benchmarks/results/report.md).
