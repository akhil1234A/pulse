"""Metrics endpoints for PulseStream API."""

import json
import time
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Query, Request
from src.common.models import EventSource, EventType
from src.common.redis_utils import (
    active_users_key,
    events_per_sec_key,
    events_today_key,
    source_count_key,
    trending_communities_key,
    type_count_key,
)

router = APIRouter(prefix="/api/v1/metrics", tags=["Metrics"])


@router.get("/realtime")
async def get_realtime_metrics(request: Request) -> Dict[str, Any]:
    """Returns current real-time aggregated metrics from Redis."""
    redis_client = getattr(request.app.state, "redis_client", None)
    if redis_client is None:
        return {
            "events_per_second": 0.0,
            "total_events_today": 0,
            "active_users": 0,
            "events_by_type": {},
            "events_by_source": {},
        }

    cache_key = "ps:cache:realtime_metrics"
    cached = await redis_client.get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass

    now_ts = time.time()
    cutoff_5s = now_ts - 5.0

    # Execute pipeline to gather stats
    pipe = redis_client.pipeline()
    pipe.get(events_today_key())
    pipe.pfcount(active_users_key())
    pipe.zcount(events_per_sec_key(), cutoff_5s, "+inf")

    event_types = [et.value for et in EventType]
    for et in event_types:
        pipe.get(type_count_key(et))

    sources = [s.value for s in EventSource]
    for src in sources:
        pipe.get(source_count_key(src))

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
        events_by_source[src] = int(results[offset] or 0)
        offset += 1

    response_data = {
        "events_per_second": eps,
        "total_events_today": total_today,
        "active_users": active_users,
        "events_by_type": events_by_type,
        "events_by_source": events_by_source,
    }

    # Short cache for realtime metrics (2 seconds)
    await redis_client.setex(cache_key, 2, json.dumps(response_data))
    return response_data


@router.get("/trending")
async def get_trending_communities(
    request: Request,
    limit: int = Query(default=10, ge=1, le=50, description="Number of top communities to return"),
) -> List[Dict[str, Any]]:
    """Returns top trending communities ordered by recent activity score."""
    redis_client = getattr(request.app.state, "redis_client", None)
    if redis_client is None:
        return []

    cache_key = f"ps:cache:trending:{limit}"
    cached = await redis_client.get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass

    # Retrieve top communities with scores
    items = await redis_client.zrevrange(
        trending_communities_key(),
        0,
        limit - 1,
        withscores=True,
    )

    trending = []
    for rank, (comm_id, score) in enumerate(items, start=1):
        trending.append({
            "rank": rank,
            "community_id": comm_id,
            "score": int(score),
        })

    # Cache for 30 seconds as specified in Technical Spec
    await redis_client.setex(cache_key, 30, json.dumps(trending))
    return trending
