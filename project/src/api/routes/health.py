"""Health check endpoint for PulseStream API."""

import asyncio
from datetime import datetime, timezone

from confluent_kafka.admin import AdminClient
from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from src.common.config import get_settings

router = APIRouter(prefix="/api/v1", tags=["Health"])


@router.get("/health")
async def health_check(request: Request):
    """Checks the health of all dependent services: Kafka, Redis, and PostgreSQL."""
    settings = get_settings()
    components = {
        "kafka": "down",
        "redis": "down",
        "postgres": "down",
    }

    # 1. Check Redis
    redis_client = getattr(request.app.state, "redis_client", None)
    if redis_client is not None:
        try:
            pong = await asyncio.wait_for(redis_client.ping(), timeout=2.0)
            if pong:
                components["redis"] = "ok"
        except Exception:
            components["redis"] = "down"

    # 2. Check PostgreSQL
    db_pool = getattr(request.app.state, "db_pool", None)
    if db_pool is not None:
        try:
            async with db_pool.acquire() as conn:
                val = await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=2.0)
                if val == 1:
                    components["postgres"] = "ok"
        except Exception:
            components["postgres"] = "down"

    # 3. Check Kafka
    def _check_kafka() -> bool:
        try:
            admin = AdminClient(
                {
                    "bootstrap.servers": settings.kafka_bootstrap_servers,
                    "socket.timeout.ms": 2000,
                }
            )
            metadata = admin.list_topics(timeout=2.0)
            return metadata is not None
        except Exception:
            return False

    loop = asyncio.get_running_loop()
    try:
        kafka_ok = await asyncio.wait_for(loop.run_in_executor(None, _check_kafka), timeout=3.0)
        if kafka_ok:
            components["kafka"] = "ok"
    except Exception:
        components["kafka"] = "down"

    all_healthy = all(v == "ok" for v in components.values())
    overall_status = "healthy" if all_healthy else "degraded"
    status_code = status.HTTP_200_OK if all_healthy else status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(
        status_code=status_code,
        content={
            "status": overall_status,
            "components": components,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )
