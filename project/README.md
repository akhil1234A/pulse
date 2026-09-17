# PulseStream

PulseStream is a high-throughput, real-time data platform that ingests continuous streams of community events from Wikipedia and synthetic simulators, processes them through distributed stream workers (aggregation, deduplication, and anomaly detection), and exposes real-time analytics through REST, WebSocket, and an interactive dashboard.

## Architecture Overview

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
   │ Apache Kafka  │   1 broker (KRaft mode, 6 partitions)
   │ Topics:       │   - pulsestream.events
   │               │   - pulsestream.events.dlq
   └──────┬───────┘
      ┌───┴───┐
      ↓       ↓
 ┌─────────┐ ┌──────────┐
 │ Agg     │ │ Anomaly  │   Consumer groups:
 │ Worker  │ │ Worker   │   - ps-aggregation
 └────┬────┘ └────┬─────┘   - ps-anomaly
      ↓           ↓
   ┌───────────────┐
   │     Redis      │   Counters, dedup, trending, anomaly state
   └───────┬───────┘
           ↓
   ┌───────────────┐
   │  PostgreSQL    │   Partitioned durable event storage
   └───────┬───────┘
           ↓
   ┌───────────────┐
   │  FastAPI API   │   REST + WebSocket + Dashboard
   └───────┬───────┘
           ↓
   ┌───────────────┐
   │   Dashboard    │   Vanilla HTML/CSS/JS (served by FastAPI)
   └───────────────┘
```

## Credentials & Access Endpoints

| Service / Interface | URL / Host:Port | Username / Email | Password | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Grafana Monitoring** | [http://localhost:3000](http://localhost:3000) | `admin` (or `admin@localhost`) | `admin` | Auto-provisioned single dashboard under **Dashboards &rarr; PulseStream System Overview** (9 panels). |
| **Real-Time Web Dashboard** | [http://localhost:8000](http://localhost:8000) | *None* | *None* | Interactive operations dashboard with live WebSocket streaming, KPI counters, and event inspector. |
| **REST API & Swagger Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | *None* | *None* | Interactive OpenAPI Swagger UI. Alternative ReDoc at `/redoc`. Rate limited to 100 req/min. |
| **PostgreSQL Database** | `localhost:5433` (container: `5432`) | `pulsestream` | `pulsestream` | Partitioned events table, anomalies table, and DLQ. Database name: `pulsestream`. |
| **Redis Cache & State** | `localhost:6379` | *None* | *None* | Real-time counters, deduplication keys (`ps:dedup:*`), and trending communities. |
| **Prometheus Metrics** | [http://localhost:9090](http://localhost:9090) | *None* | *None* | Metrics scrape targets for all ingestion, worker, and API services. |
| **Apache Kafka Broker** | `localhost:9094` (container: `9092`) | *None* | *None* | 6-partition KRaft cluster. Events topic: `pulsestream.events`, DLQ: `pulsestream.events.dlq`. |

---

## Quick Start & Setup

### 1. Prerequisites
- Python 3.11, 3.12, or 3.13
- Docker & Docker Desktop (WSL2 backend on Windows)

### 2. Virtual Environment & Dependencies
```powershell
# Create virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # On Linux/macOS: source .venv/bin/activate

# Install dependencies in editable mode
pip install -e ".[dev]"

# Configure environment variables
Copy-Item .env.example .env     # On Linux/macOS: cp .env.example .env
```

### 3. Start Core Infrastructure
Launch Kafka, Redis, PostgreSQL, Prometheus, and Grafana:
```powershell
make up
# Or directly:
docker compose -f docker/docker-compose.yml up -d
```

---

## Service Usage & Execution

### 1. Run the Ingestion Service
Starts Wikipedia SSE real-time changes and high-throughput synthetic simulator:
```powershell
make ingest
# Or directly:
python -m src.ingestion.main
```

### 2. Run Stream Workers
Starts the distributed aggregation and anomaly detection workers:
```powershell
# Aggregation Worker (Redis counters & PostgreSQL COPY persistence)
make agg-worker
# Or: python -m src.workers.aggregation.main

# Anomaly Detection Worker (Rules engine: User flood, Community spike, Duplicate storm)
make anomaly-worker
# Or: python -m src.workers.anomaly.main
```

### 3. Run the API Server & Real-Time Dashboard
Exposes REST endpoints, WebSocket streaming, and serves the dark glassmorphism dashboard:
```powershell
make api
# Or directly:
python -m src.api.main
```
Open your browser to:
- **Operations Dashboard**: [http://localhost:8000](http://localhost:8000)
- **API Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Grafana Monitoring**: [http://localhost:3000](http://localhost:3000) (Login: `admin` / `admin`)

---

## Testing, Chaos & Benchmarks

### 1. Run Automated Unit & Integration Tests
```powershell
pytest tests/unit tests/integration -v
```
*(All 76 unit and integration tests passing)*

### 2. Run Chaos Engineering Suite
Simulates abrupt container crashes, Redis outages, malformed payload floods, and duplicate storms:
```powershell
pytest tests/chaos -v -s
```
*(All 4 chaos resilience tests passing)*

### 3. Run Multi-Tier Performance Benchmarks
Executes load tests at 1,000, 3,000, and 5,000 events/sec, collecting Prometheus latency histograms and generating a markdown report:
```powershell
# Quick mode (10s per tier)
python benchmarks/run_benchmark.py --quick

# Full production mode (30s per tier)
python benchmarks/run_benchmark.py --duration 30
```
View the report generated at: [`benchmarks/results/report.md`](benchmarks/results/report.md).

---

## Detailed Documentation
For an exhaustive, step-by-step walkthrough of architecture, CLI commands, database inspection, and troubleshooting, see the [Setup & Usage Guide](docs/SETUP_AND_USAGE.md).
