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

## Documentation & Setup Guide

For an in-depth, step-by-step walkthrough of project scaffolding, environment configuration, running services, testing, and troubleshooting, see the [Setup & Usage Guide](docs/SETUP_AND_USAGE.md).

## Quick Start (Phase 1: Foundation)

### Prerequisites
- Python 3.11+
- Docker & Docker Desktop

### 1. Set Up Virtual Environment & Dependencies
```bash
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

### 2. Start Infrastructure
```bash
make up
```
This launches:
- **Kafka** (KRaft mode): `localhost:9094` (external host access) / `kafka:9092` (internal)
- **PostgreSQL 16**: `localhost:5432` (`pulsestream` database with partition schema)
- **Redis 7**: `localhost:6379` (LRU cache & real-time state)
- **Prometheus**: `http://localhost:9090`
- **Grafana**: `http://localhost:3000` (user: `admin`, password: `admin`)

### 3. Run Tests
```bash
make test
```

### 4. Run Smoke Test
```bash
make smoke
```
