.PHONY: up down logs test bench chaos reset smoke ingestion agg-worker anomaly-worker api lint format

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

test:
	pytest tests/unit tests/integration

bench:
	python benchmarks/run_benchmark.py

chaos:
	pytest tests/chaos/

reset:
	docker compose -f docker/docker-compose.yml down -v
	docker compose -f docker/docker-compose.yml up -d

smoke:
	python smoke.py

ingestion:
	python -m src.ingestion.main

agg-worker:
	python -m src.workers.aggregation.main

anomaly-worker:
	python -m src.workers.anomaly.main

api:
	python -m src.api.main
