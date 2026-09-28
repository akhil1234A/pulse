"""WebSocket manager and broadcast loop for PulseStream dashboard."""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.common.metrics import API_WEBSOCKET_CONNECTIONS
from src.common.models import EventSource, EventType
from src.common.redis_utils import (
    active_users_key,
    cross_source_alerts_key,
    events_per_sec_key,
    events_today_key,
    hot_topics_key,
    recent_anomalies_key,
    recent_events_key,
    source_count_key,
    trending_communities_key,
    type_count_key,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ws", tags=["WebSocket"])

MAX_WEBSOCKET_CONNECTIONS = 50


class ConnectionManager:
    """Manages active WebSocket dashboard clients and broadcasts updates."""

    def __init__(self, max_connections: int = MAX_WEBSOCKET_CONNECTIONS) -> None:
        self.max_connections = max_connections
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> bool:
        """Accepts a WebSocket connection if within capacity limit."""
        async with self._lock:
            if len(self.active_connections) >= self.max_connections:
                logger.warning(
                    "WebSocket connection rejected: reached maximum limit of %d",
                    self.max_connections,
                )
                await websocket.close(code=1008, reason="Connection limit exceeded")
                return False

            await websocket.accept()
            self.active_connections.add(websocket)
            API_WEBSOCKET_CONNECTIONS.set(len(self.active_connections))
            logger.info("WebSocket connected. Total active: %d", len(self.active_connections))
            return True

    async def disconnect(self, websocket: WebSocket) -> None:
        """Removes a WebSocket connection and updates gauge."""
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
                API_WEBSOCKET_CONNECTIONS.set(len(self.active_connections))
                logger.info(
                    "WebSocket disconnected. Total active: %d", len(self.active_connections)
                )

    async def broadcast(self, data: Dict[str, Any]) -> None:
        """Sends data concurrently to all active connections, cleaning up broken ones."""
        if not self.active_connections:
            return

        message_str = json.dumps(data)
        async with self._lock:
            connections = list(self.active_connections)

        disconnected: List[WebSocket] = []

        async def _send(ws: WebSocket):
            try:
                await ws.send_text(message_str)
            except Exception:
                disconnected.append(ws)

        await asyncio.gather(*[_send(ws) for ws in connections], return_exceptions=True)

        if disconnected:
            async with self._lock:
                for ws in disconnected:
                    if ws in self.active_connections:
                        self.active_connections.remove(ws)
                API_WEBSOCKET_CONNECTIONS.set(len(self.active_connections))


manager = ConnectionManager()


async def build_dashboard_payload(redis_client: Any) -> Dict[str, Any]:
    """Extracts real-time metrics, trending communities, recent events and anomalies from Redis."""
    now_ts = time.time()
    cutoff_5s = now_ts - 5.0

    pipe = redis_client.pipeline()
    # 1. Realtime counters
    pipe.get(events_today_key())
    pipe.pfcount(active_users_key())
    pipe.zcount(events_per_sec_key(), cutoff_5s, "+inf")

    event_types = [et.value for et in EventType]
    for et in event_types:
        pipe.get(type_count_key(et))

    sources = [s.value for s in EventSource]
    for src in sources:
        pipe.get(source_count_key(src))

    # 2. Top 5 trending communities
    pipe.zrevrange(trending_communities_key(), 0, 4, withscores=True)

    # 3. Recent events (latest 10)
    pipe.lrange(recent_events_key(), 0, 9)

    # 4. Recent anomalies (latest 5)
    pipe.lrange(recent_anomalies_key(), 0, 4)

    # 5. Cross-source intelligence alerts (latest 5)
    pipe.lrange(cross_source_alerts_key(), 0, 4)

    # 6. Top 5 hot topics across platforms
    pipe.zrevrange(hot_topics_key(), 0, 4, withscores=True)

    results = await pipe.execute()

    total_today = int(results[0] or 0)
    active_users = int(results[1] or 0)
    count_5s = int(results[2] or 0)
    eps = round(count_5s / 5.0, 2)

    offset = 3
    events_by_type = {}
    for et in event_types:
        events_by_type[et] = int(results[offset] or 0)
        offset += 1

    events_by_source = {}
    for src in sources:
        if offset < len(results) and not isinstance(results[offset], (list, dict)):
            val = results[offset]
            offset += 1
            try:
                events_by_source[src] = int(val or 0)
            except (ValueError, TypeError):
                events_by_source[src] = 0
        else:
            events_by_source[src] = 0

    raw_trending = results[offset] or []
    offset += 1

    trending = []
    for rank, (comm_id, score) in enumerate(raw_trending, start=1):
        trending.append(
            {
                "rank": rank,
                "community_id": comm_id,
                "score": int(score),
            }
        )

    raw_events = results[offset] or []
    offset += 1
    recent_events = []
    for raw_ev in raw_events:
        try:
            if isinstance(raw_ev, str):
                recent_events.append(json.loads(raw_ev))
            elif isinstance(raw_ev, dict):
                recent_events.append(raw_ev)
        except Exception:
            pass

    raw_anomalies = results[offset] or []
    offset += 1
    recent_anomalies = []
    for raw_anom in raw_anomalies:
        try:
            if isinstance(raw_anom, str):
                recent_anomalies.append(json.loads(raw_anom))
            elif isinstance(raw_anom, dict):
                recent_anomalies.append(raw_anom)
        except Exception:
            pass

    raw_alerts = results[offset] or []
    offset += 1
    cross_source_alerts = []
    for raw_al in raw_alerts:
        try:
            if isinstance(raw_al, str):
                cross_source_alerts.append(json.loads(raw_al))
            elif isinstance(raw_al, dict):
                cross_source_alerts.append(raw_al)
        except Exception:
            pass

    raw_hot_topics = results[offset] or []
    offset += 1
    hot_topics = []
    for rank, (topic, score) in enumerate(raw_hot_topics, start=1):
        hot_topics.append(
            {
                "rank": rank,
                "topic": topic,
                "score": int(score),
            }
        )

    return {
        "type": "dashboard_update",
        "timestamp": now_ts,
        "metrics": {
            "events_per_second": eps,
            "total_events_today": total_today,
            "active_users": active_users,
            "events_by_type": events_by_type,
            "events_by_source": events_by_source,
        },
        "trending": trending,
        "recent_events": recent_events,
        "recent_anomalies": recent_anomalies,
        "cross_source_alerts": cross_source_alerts,
        "hot_topics": hot_topics,
    }


async def dashboard_broadcast_task(app: Any) -> None:
    """Periodic task pushing metrics to all connected WebSocket clients every 1 second."""
    logger.info("Starting dashboard broadcast loop (1s interval)...")
    while getattr(app.state, "is_running", True):
        try:
            redis_client = getattr(app.state, "redis_client", None)
            if redis_client and manager.active_connections:
                payload = await build_dashboard_payload(redis_client)
                await manager.broadcast(payload)
        except Exception as e:
            logger.debug("Error during dashboard broadcast: %s", e)

        await asyncio.sleep(1.0)


@router.websocket("/dashboard")
async def dashboard_websocket_endpoint(websocket: WebSocket) -> None:
    """WebSocket endpoint streaming live dashboard updates."""
    connected = await manager.connect(websocket)
    if not connected:
        return

    # Immediately push current state upon connecting
    redis_client = getattr(websocket.app.state, "redis_client", None)
    if redis_client:
        try:
            initial_payload = await build_dashboard_payload(redis_client)
            await websocket.send_text(json.dumps(initial_payload))
        except Exception:
            pass

    try:
        while True:
            # Handle incoming ping messages or text from client to keep connection alive
            msg = await websocket.receive_text()
            if msg == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception:
        await manager.disconnect(websocket)
