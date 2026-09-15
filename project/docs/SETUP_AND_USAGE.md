# PulseStream — Setup, Scaffolding & Usage Guide

This guide provides step-by-step instructions on setting up, understanding, running, and testing the **PulseStream** project.

---

## 1. System Requirements & Prerequisites

- **Operating System**: Windows 10/11, macOS, or Linux.
- **Python**: Version 3.11, 3.12, or 3.13.
- **Docker**: Docker Desktop (with WSL2 backend on Windows) or Docker Engine on Linux.
- **Hardware**: Minimum 8GB RAM (16GB recommended; the Docker containers are capped at ~3.3GB RAM).
- **Make** *(Optional)*: `make` command utility. If `make` is not installed on Windows, equivalent PowerShell commands are documented in this guide.

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

## 6. Tear Down & Reset

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

---

## 7. Common Troubleshooting

| Issue | Cause | Solution |
|---|---|---|
| `failed to connect to the docker API at npipe` | Docker Desktop is not running. | Start the Docker Desktop application and wait for the status indicator to turn green. |
| `password authentication failed for user "pulsestream"` | Port conflict with a local PostgreSQL server on host port 5432. | Ensure `POSTGRES_PORT=5433` in `.env` so Docker maps `5433:5432`. |
| `Broker transport failure` | Kafka container still booting or topic not created. | Wait 10 seconds for Kafka KRaft quorum to stabilize, then rerun `smoke.py`. |
| `Execution_Policies` on PowerShell | PowerShell script execution disabled. | Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`. |
