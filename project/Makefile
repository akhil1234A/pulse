.PHONY: up down logs test bench chaos reset smoke ingestion

up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

logs:
	docker compose -f docker/docker-compose.yml logs -f

test:
	pytest tests/unit

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
