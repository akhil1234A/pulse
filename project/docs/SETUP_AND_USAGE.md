# PulseStream — Setup, Scaffolding & Usage Guide

This guide provides step-by-step instructions on setting up, understanding, running, and testing the **PulseStream** project.

---

## 1. System Requirements & Prerequisites

- **Operating System**: Windows 10/11, macOS, or Linux.
- **Python**: Version 3.11, 3.12, or 3.13.
- **Docker**: Docker Desktop (with WSL2 backend on Windows) or Docker Engine on Linux.
- **Hardware**: Minimum 8GB RAM (16GB recommended; Docker containers are capped at ~3.3GB total RAM).
- **Make** *(Optional)*: `make` command utility. If `make` is not installed on Windows, equivalent PowerShell commands are documented in this guide.

---

## 1.1 Quick Access & Credentials Reference

Here are all service URLs, ports, usernames, emails, and passwords:

| Service / Interface | URL / Endpoint | Username / Email | Password | Purpose & Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Grafana Monitoring** | [http://localhost:3000](http://localhost:3000) | `admin` (or `admin@localhost`) | `admin` | Auto-provisioned single dashboard with 9 telemetry panels under **Dashboards &rarr; PulseStream System Overview**. |
| **Real-Time Web Dashboard** | [http://localhost:8000](http://localhost:8000) | *None required* | *None required* | Interactive dark glassmorphism dashboard with live WebSocket streaming, KPI tickers, and event inspector. |
| **REST API Documentation** | [http://localhost:8000/docs](http://localhost:8000/docs) | *None required* | *None required* | Interactive OpenAPI Swagger documentation for all REST endpoints and filters. ReDoc at `/redoc`. |
| **PostgreSQL Database** | `localhost:5433` (container: `5432`) | `pulsestream` | `pulsestream` | Partitioned durable events table, anomalies table, and dead letter records. Database name: `pulsestream`. |
| **Redis Cache & State** | `localhost:6379` | *None* | *None* | LRU caching, deduplication keys (`ps:dedup:*`), counters, and sorted sets for trending communities. |
| **Prometheus Metrics** | [http://localhost:9090](http://localhost:9090) | *None required* | *None required* | Scrapes all pipeline services (`:9100`, `:9101`, `:9102`, `:8000`). |
| **Apache Kafka Broker** | `localhost:9094` (container: `9092`) | *None* | *None* | 6-partition KRaft cluster. Raw topic: `pulsestream.events`, DLQ: `pulsestream.events.dlq`. |

### Connection Commands Cheat-Sheet
```powershell
# PostgreSQL CLI (password: pulsestream)
docker exec -it pulsestream-postgres psql -U pulsestream -d pulsestream
# Or via local psql:
psql -h localhost -p 5433 -U pulsestream -d pulsestream

# Redis CLI
docker exec -it pulsestream-redis redis-cli

# Kafka Topic Lister
docker exec -it pulsestream-kafka /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list

# Kafka Consumer Group Status
docker exec -it pulsestream-kafka /usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group ps-aggregation
```

---

## 2. Step-by-Step Environment Setup

### Step 2.1: Clone or Open the Repository
Open a terminal in the project root directory:
```powershell
# Windows PowerShell
cd c:\Users\Akhil Anwar\projects\Personal\project
```

### Step 2.2: Create and Activate Virtual Environment
```powershell
# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Linux / macOS
python3 -m venv .venv
source .venv/bin/activate
```

> [!NOTE]
> If Windows PowerShell blocks script execution with `Execution_Policies`, run:
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```

### Step 2.3: Install Dependencies
Install the package in editable mode with development dependencies:
```powershell
pip install -e ".[dev]"
```

Installed core packages include:
- `pydantic` & `pydantic-settings`: Type-safe settings and data validation.
- `fastavro`: High-performance Avro binary serialization.
- `confluent-kafka`: Python client for Apache Kafka.
- `redis`: Sync and async client for Redis state management.
- `asyncpg`: High-performance asynchronous PostgreSQL driver with COPY protocol.
- `prometheus-client`: Metrics instrumentation.
- `python-json-logger`: Structured JSON logging.
- `pytest` & `pytest-asyncio`: Automated testing framework.

### Step 2.4: Environment Configuration (`.env`)
Copy the template configuration:
```powershell
# Windows
Copy-Item .env.example .env

# Linux / macOS
cp .env.example .env
```

The default `.env` is pre-configured for local testing:
```ini
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9094
KAFKA_EVENTS_TOPIC=pulsestream.events
KAFKA_DLQ_TOPIC=pulsestream.events.dlq
KAFKA_CONSUMER_GROUP=ps-aggregation

# Redis
REDIS_URL=redis://localhost:6379/0

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=pulsestream
POSTGRES_USER=pulsestream
POSTGRES_PASSWORD=pulsestream

# Logging & Monitoring
LOG_LEVEL=INFO
ENVIRONMENT=development
METRICS_PORT=9100
```

> [!IMPORTANT]
> **PostgreSQL Port 5433 vs 5432**: If your machine already has a local PostgreSQL instance running on port `5432`, the Docker container is mapped to host port `5433` to prevent port collisions. Inside the Docker network, other containers continue to communicate on port `5432`.

---

## 3. Project Scaffolding Explained

Here is the directory structure created in Phase 1 and the purpose of each component:

```text
project/
├── .env.example                 # Template for environment configuration
├── .env                         # Local active environment variables (ignored in git)
├── pyproject.toml               # Python package metadata, dependencies & tool configs
├── Makefile                     # Task runner shortcuts (up, down, test, smoke, etc.)
├── README.md                    # Project overview & architecture summary
├── smoke.py                     # Phase 1 end-to-end smoke verification script
├── docker/
│   ├── docker-compose.yml       # 5 infrastructure services with memory & CPU limits
│   └── configs/
│       ├── postgresql/
│       │   └── init.sql         # Partitioned table schema, anomalies, & DLQ
│       ├── prometheus/
│       │   └── prometheus.yml   # Prometheus scrape configs for pipeline components
│       └── grafana/
│           ├── dashboards/
│           │   └── pipeline.json# Pre-provisioned metrics dashboard
│           └── provisioning/    # Auto-loads datasources and dashboard JSON
├── src/
│   └── common/                  # Shared library used across all services
│       ├── __init__.py          # Public exports
│       ├── config.py            # Pydantic Settings reading from .env
│       ├── models.py            # PulseStreamEvent envelope and domain enums
│       ├── schemas/
│       │   ├── event.avsc       # Official Avro event schema specification
│       │   └── avro.py          # fastavro serialization, deserialization, validation
│       ├── kafka_utils.py       # Producer/consumer factories & partition key hasher
│       ├── redis_utils.py       # Sync/async Redis clients & atomic deduplication
│       ├── db.py                # asyncpg connection pool & batch COPY insert
│       ├── metrics.py           # Prometheus metric declarations
│       ├── logging.py           # Structured JSON logger with correlation ID
│       └── retry.py             # Exponential backoff + 3-state CircuitBreaker
├── tests/
│   └── unit/                    # 33 unit tests covering all common modules
│       ├── test_config.py
│       ├── test_models.py
│       ├── test_avro.py
│       ├── test_kafka_utils.py
│       ├── test_redis_utils.py
│       ├── test_db.py
│       ├── test_metrics.py
│       ├── test_logging.py
│       └── test_retry.py
└── scripts/
    ├── create_topics.sh         # Kafka topic creation script (6 partitions)
    └── smoke_test.py            # Wrapper forwarding to smoke.py
```

---

## 4. How to Run the Project

### Step 4.1: Ensure Docker Desktop is Running
Ensure Docker Desktop is started and the engine is running. Verify with:
```powershell
docker ps
```

### Step 4.2: Start Infrastructure Containers
Run with `make` or directly via `docker compose`:

```powershell
# Using make (if installed):
make up

# Or directly with Docker Compose:
docker compose -f docker/docker-compose.yml up -d
```

This launches the 5 foundation services:

| Container | Host Address | Role |
|---|---|---|
| `pulsestream-kafka` | `localhost:9094` | KRaft single broker (inter-container: `kafka:9092`) |
| `pulsestream-postgres` | `localhost:5433` | PostgreSQL 16 with daily partitioned schema |
| `pulsestream-redis` | `localhost:6379` | Redis 7 in-memory cache & deduplication |
| `pulsestream-prometheus` | `http://localhost:9090` | Time-series metrics collection |
| `pulsestream-grafana` | `http://localhost:3000` | Real-time observability dashboard (admin / admin) |

### Step 4.3: Verify Container Health
```powershell
docker compose -f docker/docker-compose.yml ps
```
All containers should display a status of `Up` (or `healthy`).

### Step 4.4: Create Kafka Topics
Topics can be created using the helper script or directly in the Kafka container:
```powershell
# Option A: Run inside the Kafka container
docker exec -it pulsestream-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic pulsestream.events --partitions 6 --replication-factor 1

docker exec -it pulsestream-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic pulsestream.events.dlq --partitions 3 --replication-factor 1

# Option B: Run bash script (WSL / Git Bash)
bash scripts/create_topics.sh localhost:9094
```

### Step 4.5: View Service Logs
```powershell
# Using make:
make logs

# Or directly:
docker compose -f docker/docker-compose.yml logs -f
```

---

## 5. How to Test the Project

### 5.1 Run Automated Unit Tests
The unit test suite validates all modules in `src/common/` with mock isolation, without requiring external Docker services to be running:

```powershell
# Using make:
make test

# Or directly via pytest:
pytest tests/unit -v
```

Expected output:
```text
============================= test session starts =============================
collected 33 items

tests\unit\test_avro.py ....                                             [ 12%]
tests\unit\test_config.py ....                                           [ 24%]
tests\unit\test_db.py ....                                               [ 36%]
tests\unit\test_kafka_utils.py ....                                      [ 48%]
tests\unit\test_logging.py ...                                           [ 57%]
tests\unit\test_metrics.py .                                             [ 60%]
tests\unit\test_models.py ....                                           [ 72%]
tests\unit\test_redis_utils.py ...                                       [ 81%]
tests\unit\test_retry.py ......                                          [100%]

============================= 33 passed in 1.21s ==============================
```

### 5.2 Run End-to-End Smoke Test
Once the Docker containers are running, execute the smoke test:

```powershell
# Using make:
make smoke

# Or directly with Python:
python smoke.py
```

The smoke test validates all 3 critical storage & streaming primitives:
1. **Redis**: Tests `SET`, `GET`, and atomic deduplication checks (`check_and_set_dedup_async`).
2. **PostgreSQL**: Tests inserting an event into the partitioned `events` table using the high-throughput `COPY` protocol, and reading it back.
3. **Kafka**: Produces 100 Avro-serialized `PulseStreamEvent` messages to `pulsestream.events`, then consumes and deserializes all 100 events, confirming zero data loss and schema fidelity.

Expected output:
```text
==================================================
        PulseStream Phase 1 Smoke Test           
==================================================
Kafka Bootstrap Servers: localhost:9094
Redis URL:               redis://localhost:6379/0
Postgres DSN:            postgresql://pulsestream:pulsestream@localhost:5433/pulsestream
==================================================

[1/3] Testing Redis...
  [PASS] Redis SET/GET and dedup verified successfully.

[2/3] Testing PostgreSQL...
  [PASS] PostgreSQL COPY insert and SELECT verified successfully.

[3/3] Testing Kafka (producing and consuming 100 Avro events)...
  -> Producing 100 events to topic 'pulsestream.events'...
  -> Delivered 100/100 messages to Kafka.
  -> Consuming and deserializing events...
  [PASS] Successfully consumed and verified all 100 Avro events.

----------------- SMOKE TEST SUMMARY -----------------
Redis:      PASS
PostgreSQL: PASS
Kafka:      PASS
------------------------------------------------------
ALL PHASE 1 SMOKE TESTS PASSED!
```

---

## 6. Phase 2: Ingestion Service (Run & Test)

The Ingestion Service ingests live events concurrently from **Wikipedia Recent Changes** (real-world SSE stream) and the **EventSimulator** (synthetic event generator with Zipf skew).

### 6.1 Running the Ingestion Service

#### Option A: Running locally with Python
```powershell
# Using make:
make ingestion

# Or directly:
python -m src.ingestion.main
```
Output will display structured JSON logs:
```json
{"timestamp": "2026-09-15 23:18:38", "level": "INFO", "logger": "pulsestream.ingestion", "message": "PulseStream Ingestion Service running with 2 connector(s)."}
{"timestamp": "2026-09-15 23:18:39", "level": "INFO", "logger": "src.ingestion.connectors.wikipedia", "message": "Connected to Wikimedia EventStreams successfully."}
```
Prometheus metrics will be exposed at: `http://localhost:9100/metrics`.

#### Option B: Running in Docker
```powershell
docker compose -f docker/docker-compose.yml up -d --build ingestion
```

### 6.2 Testing the Ingestion Service

#### 1. Run Ingestion Unit Tests
```powershell
pytest tests/unit/test_simulator.py tests/unit/test_wikipedia.py tests/unit/test_ingestion_pipeline.py -v
```

#### 2. Run Live Kafka Integration Test
Validates that events produced by the simulator are ingested, normalized, Avro-validated, published to Kafka, and consumed back without loss:
```powershell
pytest tests/integration/test_ingestion_kafka.py -v
```

#### 3. Monitor Live Events in Real Time
While the ingestion service is running in one terminal, open another terminal to watch the live event stream arriving from Kafka:
```powershell
# Watch normalized events on the main topic:
python scripts/consume_events.py

# Watch events on the Dead Letter Queue:
python scripts/consume_events.py --topic pulsestream.events.dlq
```
Sample output:
```text
[0001] [WIKIPEDIA] POST_CREATED     | community: enwiki          | user: EditorBeta           | id: 7b36f1c4
[0002] [SIMULATOR] COMMENT_CREATED  | community: programming     | user: usr_0012             | id: 3a98c2d1
[0003] [WIKIPEDIA] USER_JOINED      | community: commonswiki     | user: NewUser42            | id: 9e51c8f0
[0004] [SIMULATOR] REACTION_ADDED   | community: technology      | user: usr_0003             | id: 1f82b7a9
```

#### 4. Inspect Ingestion Prometheus Metrics
Check real-time counter metrics:
```powershell
curl http://localhost:9100/metrics
```
Look for:
- `ingestion_events_total{event_type="...",source="..."}`
- `ingestion_events_rejected_total{error_type="..."}`

---

## 7. Phase 3: Workers (Run & Test)

Phase 3 introduces the **Aggregation Worker** and the **Anomaly Detection Worker**. Both workers consume from `pulsestream.events` using dedicated consumer groups, deduplicate events against Redis, update aggregates, flush to PostgreSQL in batches, and run real-time anomaly detection rules.

### 7.1 Running the Workers

Make sure the Docker infrastructure is running:
```powershell
make up
# or: docker compose -f docker/docker-compose.yml up -d
```

#### Option A: Running Workers in Docker
Both workers run automatically in the Docker Compose network:
```powershell
docker compose -f docker/docker-compose.yml up -d --build agg-worker anomaly-worker
```
View their logs:
```powershell
docker compose -f docker/docker-compose.yml logs -f agg-worker
docker compose -f docker/docker-compose.yml logs -f anomaly-worker
```

#### Option B: Running Workers Locally with Python
You can also run them in separate terminal tabs for live debugging:

**Terminal 1: Aggregation Worker**
```powershell
make agg-worker
# or: python -m src.workers.aggregation.main
```

**Terminal 2: Anomaly Detection Worker**
```powershell
make anomaly-worker
# or: python -m src.workers.anomaly.main
```

---

### 7.2 Testing the Workers

#### 1. Run All Worker Unit Tests
```powershell
pytest tests/unit/test_base_worker.py tests/unit/test_aggregation_worker.py tests/unit/test_anomaly_rules.py tests/unit/test_anomaly_detector.py -v
```
To run all 65 unit tests in the project:
```powershell
make test
# or: pytest tests/unit
```

#### 2. Verify Health Check Endpoints
Both workers expose a lightweight HTTP `/health` check:
```powershell
# Aggregation Worker (port 9101)
curl http://localhost:9101/health

# Anomaly Worker (port 9102)
curl http://localhost:9102/health
```
Example response:
```json
{
  "status": "healthy",
  "worker_type": "aggregation",
  "group_id": "ps-aggregation",
  "redis_connected": true,
  "pg_circuit": "CLOSED",
  "redis_circuit": "CLOSED"
}
```

#### 3. Verify Redis Aggregates
While ingestion and the aggregation worker are running, inspect Redis keys using `redis-cli` (or via docker exec):
```powershell
# Check total events today
docker exec -it pulsestream-redis redis-cli GET ps:count:events_today

# Check top trending communities (sorted set)
docker exec -it pulsestream-redis redis-cli ZREVRANGE ps:trending:communities 0 5 WITHSCORES

# Check estimated active users (HyperLogLog)
docker exec -it pulsestream-redis redis-cli PFCOUNT ps:rate:active_users

# Check recent anomalies list
docker exec -it pulsestream-redis redis-cli LRANGE ps:anomalies:recent 0 5
```

#### 4. Verify PostgreSQL Events & Anomalies
Query the durable database tables:
```powershell
# Count events in the partitioned events table
docker exec -it pulsestream-postgres psql -U pulsestream -d pulsestream -c "SELECT count(*), min(timestamp), max(timestamp) FROM events;"

# Check detected anomalies
docker exec -it pulsestream-postgres psql -U pulsestream -d pulsestream -c "SELECT id, anomaly_type, severity, description, detected_at FROM anomalies ORDER BY detected_at DESC LIMIT 10;"
```

---

## 7.5 Phase 4: Running the API Server & Real-Time Dashboard

The API Server provides REST endpoints, WebSocket streaming, and serves the dark-theme glassmorphism dashboard.

### 1. Launch the API Server Locally
```powershell
# Using make:
make api

# Or directly:
python -m src.api.main
```
Or run as a containerized service with:
```powershell
docker compose -f docker/docker-compose.yml up -d api-server
```

### 2. Access the Dashboard & Documentation
- **Live Real-Time Dashboard**: Navigate to [http://localhost:8000](http://localhost:8000) in your browser.
  - Live metric KPI counters (Throughput, Events Today, Active Users, Anomalies) animate dynamically.
  - Live Event Feed streams real-time events via WebSocket with instant pause, search, and type/source filters.
  - Clicking any event opens an inspection modal displaying full JSON payload details.
  - Trending Communities and Anomaly alert feeds update continuously.
- **Interactive OpenAPI Documentation**: Visit [http://localhost:8000/docs](http://localhost:8000/docs).
- **Prometheus Metrics**: Scraped at [http://localhost:8000/metrics](http://localhost:8000/metrics).

### 3. Test REST Endpoints via CLI
```powershell
# Health check (Kafka, Redis, PostgreSQL)
curl http://localhost:8000/api/v1/health

# Real-time metrics
curl http://localhost:8000/api/v1/metrics/realtime

# Top trending communities
curl http://localhost:8000/api/v1/metrics/trending?limit=5

# Paginated & filtered events
curl "http://localhost:8000/api/v1/events?page=1&page_size=10&event_type=POST_CREATED"

# Recent anomalies
curl "http://localhost:8000/api/v1/anomalies?limit=10&severity=CRITICAL"
```


---

## 7.6 Phase 5: Observability, Chaos Engineering & Benchmarks

Phase 5 introduces enterprise-grade observability via Grafana auto-provisioning, automated fault-injection chaos testing, and multi-tier throughput benchmarking.

### 1. Grafana Single-Dashboard Observability
1. Ensure the infrastructure is running (`docker compose -f docker/docker-compose.yml up -d`).
2. Open [http://localhost:3000](http://localhost:3000) in your browser.
3. Sign in with credentials:
   - **Username / Email**: `admin`
   - **Password**: `admin`
4. In the left navigation menu, click **Dashboards** &rarr; open **PulseStream System Overview**.
5. All 9 panels visualize system health and metrics:
   - **Pipeline Throughput**: Ingestion rate vs Worker rate (ev/s)
   - **End-to-End Processing Latency**: Histograms for P50, P95, and P99 percentiles
   - **Kafka Consumer Lag**: Group lag across partitions
   - **Error Rate & Dead Letter Queue**: DLQ rate and dropped payload counts
   - **Redis Cache Activity**: Deduplication hits and command frequencies
   - **PostgreSQL Write Latency & Batch Size**: COPY protocol durations and row counts
   - **Circuit Breaker Status**: Postgres & Redis breaker states (Closed / Half-Open / Open)
   - **Active WebSocket Connections**: Current connected dashboard clients

### 2. Running the Chaos Engineering Test Suite
The chaos suite tests resilience against duplicate floods, corrupt payloads, service outages, and worker crashes:
```powershell
# Run all chaos resilience tests
pytest tests/chaos -v -s

# Or run an individual chaos test:
pytest tests/chaos/test_chaos.py -k test_duplicate_dedup -v -s
pytest tests/chaos/test_chaos.py -k test_malformed_event_dlq -v -s
pytest tests/chaos/test_chaos.py -k test_redis_unavailability -v -s
pytest tests/chaos/test_chaos.py -k test_consumer_crash_recovery -v -s
```
All chaos tests automatically clean up, unpause, and restart any affected containers upon completion.

### 3. Running the Performance Benchmark Suite
The benchmark suite executes controlled multi-tier traffic runs (1,000, 3,000, and 5,000 events/sec), samples host telemetry and Prometheus metrics, and generates a performance report:
```powershell
# Quick mode (10 seconds per tier)
python benchmarks/run_benchmark.py --quick

# Full production mode (30 seconds per tier)
python benchmarks/run_benchmark.py --duration 30

# Test a single custom target rate (e.g. 2,000 ev/s for 20s)
python benchmarks/run_benchmark.py --target-rate 2000 --duration 20
```
Upon completion, review the markdown summary generated at:
`benchmarks/results/report.md`

---

## 8. Tear Down & Reset

### Stop Infrastructure
```powershell
# Using make:
make down

# Or directly:
docker compose -f docker/docker-compose.yml down
```

### Reset Data Volumes (Clean Slate)
If you want to wipe all Kafka logs, PostgreSQL data, and Redis state:
```powershell
# Using make:
make reset

# Or directly:
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d
```
docker compose -f docker/docker-compose.yml up -d --build

---

## 9. Common Troubleshooting

| Issue | Cause | Solution |
|---|---|---|
| `failed to connect to the docker API at npipe` | Docker Desktop is not running. | Start the Docker Desktop application and wait for the status indicator to turn green. |
| `password authentication failed for user "pulsestream"` | Port conflict with a local PostgreSQL server on host port 5432. | Ensure `POSTGRES_PORT=5433` in `.env` so Docker maps `5433:5432`. |
| `Broker transport failure` | Kafka container still booting or topic not created. | Wait 10 seconds for Kafka KRaft quorum to stabilize, then rerun `smoke.py`. |
| `ModuleNotFoundError: No module named 'src'` | Running scripts without package in Python path. | Use `python -m src.<module>` or run `pip install -e .` from project root. |
| `Execution_Policies` on PowerShell | PowerShell script execution disabled. | Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`. |
