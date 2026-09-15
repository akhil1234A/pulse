# PulseStream — Development Activities

## Timeline Overview

**Estimated total: 5-6 weeks** (part-time, ~15-20 hrs/week)

```text
Week 1      ██████████  Foundation & Infrastructure
Week 2      ██████████  Ingestion Service
Week 3      ██████████  Workers (Aggregation + Anomaly)
Week 4      ██████████  API & Dashboard
Week 5      ██████████  Observability, Chaos Tests & Benchmarks
Week 6      █████       Polish & Documentation
```

---

## Phase 1: Foundation & Infrastructure (Week 1)

### Goal
Project structure, Docker infrastructure, shared library. Prove data flows through Kafka.

### Activities

#### 1.1 Project Setup
- [x] Initialize Python project with `pyproject.toml`
- [x] Set up project structure:
  ```
  pulsestream/
  ├── docker/
  │   ├── docker-compose.yml
  │   └── configs/
  │       ├── kafka/
  │       ├── postgresql/
  │       │   └── init.sql            (schema creation)
  │       ├── prometheus/
  │       │   └── prometheus.yml
  │       └── grafana/
  │           └── dashboards/
  ├── src/
  │   ├── common/
  │   │   ├── config.py               (pydantic settings)
  │   │   ├── models.py               (event models)
  │   │   ├── schemas/                 (avro schema files)
  │   │   ├── kafka_utils.py           (producer/consumer helpers)
  │   │   ├── redis_utils.py
  │   │   ├── db.py                    (asyncpg connection pool)
  │   │   ├── metrics.py               (prometheus definitions)
  │   │   ├── logging.py               (structured JSON logger)
  │   │   └── retry.py                 (backoff + circuit breaker)
  │   ├── ingestion/
  │   │   ├── main.py
  │   │   ├── connectors/
  │   │   │   ├── wikipedia.py
  │   │   │   └── simulator.py
  │   │   └── pipeline.py
  │   ├── workers/
  │   │   ├── aggregation/
  │   │   │   ├── main.py
  │   │   │   └── aggregator.py
  │   │   └── anomaly/
  │   │       ├── main.py
  │   │       ├── detector.py
  │   │       └── rules.py
  │   ├── api/
  │   │   ├── main.py
  │   │   ├── routes/
  │   │   │   ├── events.py
  │   │   │   ├── metrics.py
  │   │   │   ├── anomalies.py
  │   │   │   └── health.py
  │   │   └── websocket.py
  │   └── dashboard/
  │       ├── index.html
  │       ├── style.css
  │       └── app.js
  ├── tests/
  │   ├── unit/
  │   ├── integration/
  │   └── chaos/
  ├── benchmarks/
  │   ├── run_benchmark.py
  │   └── results/
  ├── docs/
  ├── scripts/
  │   └── create_topics.sh
  ├── Makefile
  ├── README.md
  └── .env.example
  ```
- [x] Create `Makefile`:
  ```makefile
  up:     docker compose -f docker/docker-compose.yml up -d
  down:   docker compose -f docker/docker-compose.yml down
  logs:   docker compose -f docker/docker-compose.yml logs -f
  test:   pytest tests/unit
  bench:  python benchmarks/run_benchmark.py
  chaos:  pytest tests/chaos/
  reset:  docker compose -f docker/docker-compose.yml down -v && make up
  ```

#### 1.2 Docker Compose
- [x] Write `docker-compose.yml`:
  - Kafka (KRaft mode, single broker, no Zookeeper)
  - PostgreSQL (with `init.sql` mount for schema)
  - Redis (maxmemory 256mb, allkeys-lru)
  - Prometheus (scrape config mounted)
  - Grafana (dashboard provisioning)
- [x] Create `init.sql` — events, anomalies, dead_letter_events tables + indexes
- [x] Create `prometheus.yml` — scrape targets for all services
- [x] Create `create_topics.sh` — create Kafka topics on startup
- [x] Verify: `make up` → all 5 infrastructure containers healthy

#### 1.3 Shared Library (`src/common/`)
- [x] `config.py` — Pydantic Settings (Kafka, Redis, PostgreSQL connection strings)
- [x] `models.py` — Pydantic models for event envelope
- [x] `schemas/` — Avro schema file + serialize/deserialize helpers (fastavro)
- [x] `kafka_utils.py` — producer factory, consumer factory (confluent-kafka)
- [x] `redis_utils.py` — connection pool, common operations
- [x] `db.py` — asyncpg connection pool, batch insert helper
- [x] `metrics.py` — all Prometheus metric definitions
- [x] `logging.py` — structured JSON logger with correlation ID
- [x] `retry.py` — exponential backoff with jitter + circuit breaker class
- [x] Unit tests for all common modules

#### 1.4 Smoke Test
- [x] Script: produce 100 Avro-serialized events to Kafka
- [x] Script: consume and deserialize those events
- [x] Verify Redis SET/GET works
- [x] Verify PostgreSQL INSERT/SELECT works

### Definition of Done
```text
$ make up           → 5 infra containers healthy
$ make test         → common/ unit tests pass
$ python smoke.py   → 100 events roundtrip through Kafka
```

---

## Phase 2: Ingestion Service (Week 2)

### Goal
Wikipedia SSE + Simulator producing validated events to Kafka.

### Activities

#### 2.1 Event Simulator
- [ ] `EventSimulator` class with async generator pattern
- [ ] Modes: steady (default 1000/sec), burst (10x for 30s), chaos, ramp
- [ ] Chaos features:
  - Duplicate injection (configurable %, default 5%)
  - Malformed events (missing fields, bad types, default 2%)
  - Out-of-order timestamps
- [ ] User/community distribution (Zipf for realistic skew)
- [ ] Unit tests for all modes

#### 2.2 Wikipedia Connector
- [ ] SSE client using `aiohttp` (connect to `stream.wikimedia.org`)
- [ ] Event type mapping:
  - `edit` → `POST_CREATED`
  - `new` → `POST_CREATED`
  - `log` → `USER_JOINED`
  - `categorize` → `REACTION_ADDED`
- [ ] Auto-reconnect with exponential backoff on disconnect
- [ ] Filter: only main namespace + user namespace
- [ ] Unit tests with mocked SSE responses

#### 2.3 Ingestion Pipeline
- [ ] `IngestionPipeline` class:
  - `receive()` → `normalize()` → `validate_avro()` → `assign_id()` → `publish()`
- [ ] Avro validation with descriptive error messages
- [ ] DLQ routing for invalid events
- [ ] Prometheus metrics: `ingestion_events_total`, `ingestion_events_rejected_total`
- [ ] Structured logging per event

#### 2.4 Service Entry Point
- [ ] `main.py` — runs Wikipedia + simulator concurrently (asyncio)
- [ ] Graceful shutdown (SIGTERM → drain in-flight events)
- [ ] Dockerfile
- [ ] Add to docker-compose.yml
- [ ] Integration test: run ingestion → verify events in Kafka

### Definition of Done
```text
$ make up  (with ingestion service)
→ Kafka consumer shows Wikipedia events arriving in real-time
→ Simulator produces events at configurable rate
→ Invalid events appear in DLQ topic
```

---

## Phase 3: Workers (Week 3)

### Goal
Aggregation worker (counters + storage) and anomaly detection worker.

### Activities

#### 3.1 Base Worker Framework
- [ ] `BaseWorker` class:
  - Kafka consumer with manual offset commit
  - Batch processing loop (poll → process → commit)
  - Dedup check via Redis (`SISMEMBER`/`SETEX`)
  - Error handling: retry with backoff → DLQ after max retries
  - Graceful shutdown
  - Health check HTTP endpoint (`/health`)
  - Prometheus metrics integration
- [ ] Unit tests for worker lifecycle and error handling

#### 3.2 Aggregation Worker
- [ ] Counter updates (Redis pipeline for batch efficiency):
  - `events_today` → INCR
  - `type:{type}` → INCR
  - `source:{source}` → INCR
  - `community:{id}` → INCR
- [ ] Active users → PFADD (HyperLogLog)
- [ ] Events/sec → ZADD to sorted set, trim entries >60s
- [ ] Trending communities → ZINCRBY
- [ ] Batch PostgreSQL inserts:
  - Buffer events in memory
  - Flush on 1000 events OR 5 seconds (whichever first)
  - Use COPY protocol via asyncpg
  - Circuit breaker on PostgreSQL connection
- [ ] Dedup: mark processed event_ids in Redis (TTL 1h)
- [ ] Unit tests with mocked Redis/PostgreSQL
- [ ] Integration test: events → Kafka → worker → verify Redis counters + PostgreSQL rows

#### 3.3 Anomaly Detection Worker
- [ ] Rule engine framework (list of pluggable rules)
- [ ] **User Flood rule**:
  - ZADD per-user sorted set with timestamps
  - Count events in last 60s
  - Trigger at >50 events
- [ ] **Community Spike rule**:
  - Compare 1-min rate vs 1-hour rolling average
  - Trigger at >10x average
- [ ] **Duplicate Storm rule**:
  - Track dedup hit rate per batch
  - Trigger at >5% duplicates
- [ ] Write anomalies to PostgreSQL `anomalies` table
- [ ] Publish anomaly notifications to Redis (for dashboard)
- [ ] Unit tests for each rule with edge cases
- [ ] Integration test: inject bot-like events → verify anomaly detected

#### 3.4 Dockerize Workers
- [ ] Dockerfile (shared base image for both workers)
- [ ] Add both workers to docker-compose.yml
- [ ] Health check configuration

### Definition of Done
```text
$ make up  (full pipeline running)
→ Redis: counters updating, trending list populated
→ PostgreSQL: events table growing with batch inserts
→ Inject 100 rapid events from same user → USER_FLOOD anomaly detected
→ Kill agg-worker → restart → resumes from last committed offset
```

---

## Phase 4: API & Dashboard (Week 4)

### Goal
REST API, WebSocket, and real-time dashboard.

### Activities

#### 4.1 FastAPI REST Endpoints
- [ ] `GET /api/v1/health` — check Kafka, Redis, PostgreSQL
- [ ] `GET /api/v1/metrics/realtime` — events/sec, active users, totals (from Redis)
- [ ] `GET /api/v1/metrics/trending` — top communities (from Redis)
- [ ] `GET /api/v1/events` — paginated + filtered (PostgreSQL):
  - Query params: `type`, `source`, `community`, `from`, `to`, `page`, `limit`
- [ ] `GET /api/v1/events/{event_id}` — single event
- [ ] `GET /api/v1/anomalies` — recent anomalies (PostgreSQL)
- [ ] Response caching in Redis (TTL 30s for aggregation queries)
- [ ] Rate limiting middleware (100 req/min per IP)
- [ ] CORS middleware
- [ ] Prometheus metrics for request latency
- [ ] Auto-generated OpenAPI docs at `/docs`
- [ ] Unit tests for all endpoints

#### 4.2 WebSocket Endpoint
- [ ] `WS /api/v1/ws/dashboard` — push updates every 1 second
- [ ] Aggregate real-time data from Redis each cycle
- [ ] Push: events/sec, active users, totals, trending, recent anomalies
- [ ] Handle connect/disconnect/heartbeat
- [ ] Cap at 50 concurrent connections

#### 4.3 Dashboard UI
- [ ] Vanilla HTML/CSS/JS (no framework, no build step)
- [ ] Layout:
  ```
  ┌─────────────────────────────────────────┐
  │  PulseStream                 ● Live     │
  ├──────────┬──────────┬──────────┐        │
  │ Events/s │ Active   │ Events   │        │
  │  1,241   │ Users    │ Today    │        │
  │          │ 4,302    │ 2.1M     │        │
  ├──────────┴──────────┴──────────┤        │
  │ ┌─ Trending ─┐ ┌─ Anomalies ─┐│        │
  │ │ prog...  ↑ │ │ FLOOD HIGH  ││        │
  │ │ tech...  → │ │ 14:32:01    ││        │
  │ └────────────┘ └─────────────┘│        │
  │ ┌─ Live Event Stream ────────┐│        │
  │ │ 14:32:05 POST wiki prog.. ││        │
  │ │ 14:32:04 COMMENT sim ai.. ││        │
  │ └────────────────────────────┘│        │
  │ ┌─ Search ───────────────────┐│        │
  │ │ [Type▼] [Source▼] [Search] ││        │
  │ │ Results table...           ││        │
  │ └────────────────────────────┘│        │
  └───────────────────────────────────────┘
  ```
- [ ] WebSocket connection with auto-reconnect
- [ ] Smooth number animations (counter transitions)
- [ ] Live event stream (last 50 events, auto-scroll)
- [ ] Trending with trend indicators (↑ ↓ →)
- [ ] Anomaly alerts with severity colors
- [ ] Search form → REST API → results table
- [ ] Connection status indicator (● Live / ● Disconnected)
- [ ] Dark theme, modern design
- [ ] Serve as static files from FastAPI (`app.mount("/", StaticFiles(...))`)

#### 4.4 Dockerize API
- [ ] Dockerfile (uvicorn, single worker)
- [ ] Add to docker-compose.yml
- [ ] Mount dashboard static files

### Definition of Done
```text
$ open http://localhost:8000
→ Dashboard shows live metrics updating via WebSocket
→ Counters animate smoothly
→ Trending list updates as communities change
→ Search returns filtered results
→ /docs shows OpenAPI documentation
```

---

## Phase 5: Observability, Chaos & Benchmarks (Week 5)

### Goal
Grafana dashboard, chaos tests, benchmark report. This is what makes the project impressive.

### Activities

#### 5.1 Grafana Dashboard
- [ ] Provision single dashboard JSON:
  - Events/sec throughput graph
  - End-to-end latency histogram (P50/P95/P99)
  - Kafka consumer lag per group
  - Error rate + DLQ events
  - Redis memory + ops/sec
  - PostgreSQL write latency
  - Circuit breaker states
- [ ] Auto-provisioned on startup (no manual config)
- [ ] Verify all panels populate with real data

#### 5.2 Chaos Test Framework
- [ ] `ChaosRunner` class using Docker SDK:
  ```python
  def kill_container(self, service)
  def pause_container(self, service)
  def inject_duplicates(self, count)
  def inject_malformed(self, count)
  ```

#### 5.3 Chaos Tests (4 tests)
- [ ] **Test 1: Consumer Crash Recovery**
  - Pipeline at 500 events/sec
  - Kill agg-worker container
  - Wait 30s, restart
  - Assert: no events lost (count before == count after)
  - Assert: worker resumes from last committed offset

- [ ] **Test 2: Duplicate Dedup**
  - Inject 100 duplicate events (same event_id)
  - Assert: PostgreSQL has exactly 1 copy of each
  - Assert: Redis dedup counters reflect hits

- [ ] **Test 3: Malformed Event DLQ**
  - Inject 50 malformed events via simulator chaos mode
  - Assert: all appear in dead_letter_events table
  - Assert: valid events unaffected

- [ ] **Test 4: Redis Unavailability**
  - Pause Redis container
  - Assert: workers continue processing (degraded mode)
  - Unpause Redis
  - Assert: counters resume updating

#### 5.4 Benchmark Suite
- [ ] `BenchmarkRunner`:
  - Start simulator at target rate
  - Collect Prometheus metrics for duration
  - Generate results (throughput, latency percentiles, lag, resource usage)

- [ ] **Run 1**: 1,000 events/sec, 5 minutes — baseline
- [ ] **Run 2**: 3,000 events/sec, 5 minutes — moderate
- [ ] **Run 3**: 5,000 events/sec, 5 minutes — target

- [ ] Generate `benchmarks/results/report.md`:
  - System specs (CPU, RAM, Docker resource limits)
  - Methodology
  - Results table per run
  - Bottleneck analysis

### Definition of Done
```text
$ open http://localhost:3000  (Grafana)
→ Dashboard shows live pipeline metrics

$ make chaos
→ 4 chaos tests pass

$ make bench
→ Benchmark report generated with 3 runs
```

---

## Phase 6: Polish & Documentation (Week 6)

### Goal
Portfolio-ready: clean code, great README, optional blog post.

### Activities

#### 6.1 Code Quality
- [ ] Linting (ruff) and formatting (black)
- [ ] Type hints on all public APIs
- [ ] Docstrings on classes and public methods
- [ ] Remove debug code and TODOs
- [ ] Ensure no secrets committed (`.env.example` only)

#### 6.2 Testing
- [ ] Unit test coverage > 70% on core modules
- [ ] Integration tests documented in README
- [ ] Chaos tests documented in README

#### 6.3 README.md
- [ ] Project overview (1 paragraph)
- [ ] Architecture diagram (text-based, from tech spec)
- [ ] Quick start:
  ```bash
  git clone ... && cd pulsestream
  cp .env.example .env
  make up
  # Open http://localhost:8000 (dashboard)
  # Open http://localhost:3000 (grafana)
  ```
- [ ] Feature list (what it does, link to relevant code)
- [ ] Benchmark results summary
- [ ] Chaos test results summary
- [ ] Technology decisions (why Kafka KRaft, why Avro, why HyperLogLog)
- [ ] Screenshots (dashboard + Grafana)

#### 6.4 Architecture Documentation
- [ ] `docs/architecture.md` with Mermaid diagrams:
  - Data flow
  - Failure handling
  - Consumer group topology
- [ ] 3-4 ADRs (Architecture Decision Records):
  - Why single Kafka broker (KRaft)
  - Why manual commits + idempotent processing
  - Why merge storage into aggregation worker
  - Why HyperLogLog for active users

#### 6.5 Blog Post (Optional but High Impact)
- [ ] 1500-2000 words covering:
  - The problem: what happens at 5K events/sec?
  - Architecture and why
  - Hardest problems: dedup, backpressure, circuit breakers
  - Chaos testing results
  - Benchmark results
  - What I'd change for production scale
- [ ] Publish on dev.to or personal blog

### Definition of Done
```text
A stranger clones the repo:
$ git clone && cd pulsestream && cp .env.example .env && make up
→ Dashboard live at localhost:8000
→ Grafana live at localhost:3000
→ README explains everything
→ Total RAM < 6GB
```

---

## Dependency Graph

```
Phase 1 (Foundation) ──→ Phase 2 (Ingestion) ──→ Phase 3 (Workers)
                                                       │
                                    ┌──────────────────┤
                                    ↓                  ↓
                              Phase 4            Phase 5
                              (API &             (Observability,
                              Dashboard)         Chaos, Benchmarks)
                                    │                  │
                                    └────────┬─────────┘
                                             ↓
                                       Phase 6 (Polish)
```

> **Phases 4 and 5 can overlap** — API/Dashboard and Observability/Chaos are independent work streams.

---

## Risk-Adjusted Schedule

| Phase | Optimistic | Expected | Pessimistic | Risk Factor |
|---|---|---|---|---|
| 1. Foundation | 3 days | 5 days | 7 days | Kafka KRaft config on Windows |
| 2. Ingestion | 3 days | 5 days | 7 days | Wikipedia SSE quirks |
| 3. Workers | 5 days | 7 days | 10 days | Highest complexity (Redis + PG + Kafka) |
| 4. API & Dashboard | 4 days | 6 days | 8 days | UI polish always takes longer than expected |
| 5. Observability + Chaos + Bench | 4 days | 6 days | 8 days | Chaos tests can be flaky |
| 6. Polish | 3 days | 5 days | 7 days | Blog writing |
| **Total** | **~3.5 weeks** | **~5 weeks** | **~7 weeks** | |

---

## What Was Cut (and Why)

| Removed | Why | Impact |
|---|---|---|
| 3 Kafka brokers → 1 (KRaft) | Saves ~3GB RAM, eliminates Zookeeper | Replication not demonstrated, but partitioning/consumer groups still work |
| GitHub Events connector | Rate-limited to ~2.5 events/sec max — not useful for throughput | Wikipedia SSE provides better real-time volume |
| pgbouncer | asyncpg has built-in connection pooling | One fewer container |
| Nginx | FastAPI can serve static files and handle single-instance traffic | One fewer container |
| Separate storage worker | Merged into aggregation worker | One fewer container, simpler |
| 3 Grafana dashboards → 1 | One comprehensive dashboard is sufficient | Less config maintenance |
| 7 chaos tests → 4 | 4 tests cover the most impactful scenarios | Less test maintenance |
| 5 benchmark runs → 3 | 3 runs (1K/3K/5K) tell the performance story | Faster benchmarking |
| Demo video | Time-consuming, README + screenshots suffice | Can add later |
| Kubernetes | Requires real cluster, overkill for portfolio | Docker Compose is honest |
| OpenSearch | PostgreSQL GIN indexes are sufficient | One fewer technology |

---

## Daily Workflow

```text
1. Pick next unchecked item from current phase
2. Write test first (unit test for the component)
3. Implement the feature
4. make test → verify unit tests pass
5. make up → verify integration manually
6. Commit:
     feat(ingestion): add Wikipedia SSE connector
     fix(worker): handle Redis timeout gracefully
     test(chaos): add consumer crash recovery test
7. Check off completed items in this document
```
