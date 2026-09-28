"""FastAPI application entry point for PulseStream API & Dashboard."""

import asyncio
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Deque, Dict

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src.api.routes.anomalies import router as anomalies_router
from src.api.routes.events import router as events_router
from src.api.routes.health import router as health_router
from src.api.routes.intelligence import router as intelligence_router
from src.api.routes.metrics import router as metrics_router
from src.api.websocket import dashboard_broadcast_task
from src.api.websocket import router as websocket_router
from src.common.config import get_settings
from src.common.db import close_db_pool, create_db_pool
from src.common.logging import get_logger, setup_logging
from src.common.metrics import API_REQUEST_LATENCY_SECONDS
from src.common.redis_utils import get_async_redis_client

logger = get_logger(__name__)


# Rate Limiter: In-memory sliding window (100 req/min per IP)
class SlidingWindowRateLimiter:
    def __init__(self, limit: int = 100, window_seconds: float = 60.0) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self.requests: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def is_allowed(self, client_ip: str) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        async with self._lock:
            timestamps = self.requests[client_ip]
            while timestamps and timestamps[0] < cutoff:
                timestamps.popleft()
            if len(timestamps) >= self.limit:
                return False
            timestamps.append(now)
            return True


rate_limiter = SlidingWindowRateLimiter(limit=100, window_seconds=60.0)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manages lifecycle of application resources (Redis, PostgreSQL, broadcast loop)."""
    setup_logging()
    settings = get_settings()
    logger.info("Starting PulseStream API Server on %s:%d...", settings.api_host, settings.api_port)

    # 1. Initialize Async Redis
    try:
        app.state.redis_client = get_async_redis_client()
        await app.state.redis_client.ping()
        logger.info("Connected to Redis at %s", settings.redis_url)
    except Exception as e:
        logger.warning("Could not connect to Redis at startup: %s", e)
        app.state.redis_client = None

    # 2. Initialize PostgreSQL Pool
    try:
        app.state.db_pool = await create_db_pool(min_size=2, max_size=10)
        logger.info(
            "Connected to PostgreSQL at %s:%d", settings.postgres_host, settings.postgres_port
        )
    except Exception as e:
        logger.warning("Could not connect to PostgreSQL at startup: %s", e)
        app.state.db_pool = None

    # 3. Start WebSocket broadcast loop
    app.state.is_running = True
    broadcast_task = asyncio.create_task(dashboard_broadcast_task(app))

    yield

    # Shutdown
    logger.info("Shutting down PulseStream API Server...")
    app.state.is_running = False
    broadcast_task.cancel()
    try:
        await broadcast_task
    except asyncio.CancelledError:
        pass

    if app.state.redis_client:
        await app.state.redis_client.aclose()

    if app.state.db_pool:
        await close_db_pool(app.state.db_pool)

    logger.info("PulseStream API Server resources released.")


app = FastAPI(
    title="PulseStream API",
    description="Real-Time Community Event Streaming & Analytics Platform API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def rate_limit_and_metrics_middleware(request: Request, call_next):
    """Tracks endpoint latency and enforces 100 req/min sliding window rate limit per client IP."""
    path = request.url.path

    # Bypass rate limiting for metrics, health, docs, and static assets
    bypass_rate_limit = (
        path.startswith("/metrics")
        or path.startswith("/api/v1/health")
        or path.startswith("/docs")
        or path.startswith("/redoc")
        or path.startswith("/openapi.json")
        or "." in path.split("/")[-1]  # Static assets (css, js, html, etc.)
    )

    if not bypass_rate_limit:
        client_ip = request.client.host if request.client else "unknown"
        if not await rate_limiter.is_allowed(client_ip):
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Rate limit exceeded. Maximum 100 requests per minute."},
            )

    start_time = time.monotonic()
    response = await call_next(request)
    duration = time.monotonic() - start_time

    # Record Prometheus metrics for API requests
    try:
        endpoint = request.scope.get("route").path if request.scope.get("route") else path
        API_REQUEST_LATENCY_SECONDS.labels(method=request.method, endpoint=endpoint).observe(
            duration
        )
    except Exception:
        pass

    return response


# Include Routers
app.include_router(health_router)
app.include_router(metrics_router)
app.include_router(events_router)
app.include_router(anomalies_router)
app.include_router(intelligence_router)
app.include_router(websocket_router)


# Prometheus /metrics endpoint
@app.get("/metrics", tags=["Observability"])
def prometheus_metrics() -> Response:
    """Exposes Prometheus scrape metrics."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# Mount Dashboard static files
dashboard_dir = Path(__file__).resolve().parent.parent / "dashboard"
if not dashboard_dir.exists():
    dashboard_dir.mkdir(parents=True, exist_ok=True)

app.mount("/", StaticFiles(directory=str(dashboard_dir), html=True), name="dashboard")


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "src.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
