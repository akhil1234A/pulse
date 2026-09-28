"""Intelligence query endpoints for PulseStream API.

Surfaces cross-source correlation alerts and hot topics detected across multiple streaming platforms.
"""

import json
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from src.common.models import EventSource
from src.common.redis_utils import (
    cross_source_alerts_key,
    events_per_sec_key,
    events_today_key,
    hot_topics_key,
    source_count_key,
    topic_source_key,
)
from src.workers.intelligence.cross_source import normalize_topic

router = APIRouter(prefix="/api/v1/intelligence", tags=["Intelligence"])


@router.get("/cross-source")
async def get_cross_source_alerts(
    request: Request,
    limit: int = Query(default=20, ge=1, le=100, description="Maximum alerts to return"),
) -> List[Dict[str, Any]]:
    """Retrieves recent cross-source surge alerts from Redis cache or PostgreSQL."""
    redis_client = getattr(request.app.state, "redis_client", None)
    alerts: List[Dict[str, Any]] = []

    # 1. Try fetching from Redis fast cache first
    if redis_client:
        try:
            raw_alerts = await redis_client.lrange(cross_source_alerts_key(), 0, limit - 1)
            for item in raw_alerts:
                try:
                    alerts.append(json.loads(item))
                except Exception:
                    pass
        except Exception:
            pass

    if alerts:
        return alerts[:limit]

    # 2. Fallback to PostgreSQL anomalies table
    db_pool = getattr(request.app.state, "db_pool", None)
    if not db_pool:
        return []

    try:
        query = """
            SELECT id, anomaly_type, severity, description, details, detected_at, event_ids
            FROM anomalies
            WHERE anomaly_type = 'CROSS_SOURCE_CORRELATION'
            ORDER BY detected_at DESC
            LIMIT $1
        """
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(query, limit)

        for row in rows:
            details = row["details"]
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except Exception:
                    details = {}

            alerts.append(
                {
                    "id": row["id"],
                    "topic": details.get("topic") if isinstance(details, dict) else "Unknown",
                    "wikipedia_count": details.get("wikipedia_count", 0) if isinstance(details, dict) else 0,
                    "hackernews_count": details.get("hackernews_count", 0) if isinstance(details, dict) else 0,
                    "github_count": details.get("github_count", 0) if isinstance(details, dict) else 0,
                    "reddit_count": details.get("reddit_count", 0) if isinstance(details, dict) else 0,
                    "severity": row["severity"],
                    "description": row["description"],
                    "detected_at": row["detected_at"].isoformat() if row["detected_at"] else None,
                    "event_ids": [str(eid) for eid in (row["event_ids"] or [])],
                }
            )
    except Exception:
        pass

    return alerts


@router.get("/hot-topics")
async def get_hot_topics(
    request: Request,
    limit: int = Query(default=10, ge=1, le=50, description="Maximum hot topics to return"),
) -> List[Dict[str, Any]]:
    """Retrieves currently hot topics trending across platforms."""
    redis_client = getattr(request.app.state, "redis_client", None)
    if not redis_client:
        return []

    try:
        results = await redis_client.zrevrange(hot_topics_key(), 0, limit - 1, withscores=True)
        hot_topics = []
        for rank, (topic, score) in enumerate(results, start=1):
            hot_topics.append(
                {
                    "rank": rank,
                    "topic": topic,
                    "score": int(score),
                }
            )
        return hot_topics
    except Exception:
        return []


@router.get("/pulse-summary")
async def get_pulse_summary(request: Request) -> Dict[str, Any]:
    """Generates a high-level executive briefing across all streaming sources and topics."""
    redis_client = getattr(request.app.state, "redis_client", None)
    if not redis_client:
        return {
            "status": "idle",
            "executive_insight": "PulseStream intelligence engine awaiting active connection.",
            "top_topics": [],
            "source_health": {},
            "active_anomalies_count": 0,
        }

    now_ts = time.time()
    cutoff_5s = now_ts - 5.0

    try:
        pipe = redis_client.pipeline()
        pipe.zrevrange(hot_topics_key(), 0, 4, withscores=True)
        pipe.lrange(cross_source_alerts_key(), 0, 4)
        pipe.zcount(events_per_sec_key(), cutoff_5s, "+inf")
        pipe.get(events_today_key())

        sources = [s.value for s in EventSource]
        for src in sources:
            pipe.get(source_count_key(src))

        results = await pipe.execute()

        raw_topics = results[0] or []
        raw_alerts = results[1] or []
        eps_count = int(results[2] or 0)
        eps = round(eps_count / 5.0, 2)
        total_today = int(results[3] or 0)

        offset = 4
        source_counts: Dict[str, int] = {}
        for src in sources:
            if offset < len(results):
                try:
                    source_counts[src] = int(results[offset] or 0)
                except (ValueError, TypeError):
                    source_counts[src] = 0
                offset += 1
            else:
                source_counts[src] = 0

        # Format top topics
        top_topics = []
        for rank, (topic, score) in enumerate(raw_topics, start=1):
            top_topics.append({"rank": rank, "topic": topic, "momentum_score": int(score)})

        # Parse alerts
        parsed_alerts = []
        for a in raw_alerts:
            try:
                parsed_alerts.append(json.loads(a))
            except Exception:
                pass

        # Synthesize executive insight
        active_source_names = [s for s, count in source_counts.items() if count > 0]
        if parsed_alerts:
            top_alert = parsed_alerts[0]
            executive_insight = (
                f"Surge Alert: '{top_alert.get('topic')}' is generating correlated activity. "
                f"Throughput is {eps} events/sec across {len(active_source_names)} active platforms."
            )
        elif top_topics:
            leading = top_topics[0]
            executive_insight = (
                f"Leading momentum on '{leading['topic']}' (score {leading['momentum_score']}). "
                f"System processing {eps} events/sec ({total_today} events today)."
            )
        else:
            executive_insight = (
                f"All streams nominal at {eps} events/sec. "
                f"Monitoring Wikipedia, Hacker News, GitHub, and Reddit for emerging trends."
            )

        return {
            "status": "active" if eps > 0 or total_today > 0 else "idle",
            "executive_insight": executive_insight,
            "events_per_second": eps,
            "total_events_today": total_today,
            "top_topics": top_topics,
            "source_health": source_counts,
            "active_anomalies_count": len(parsed_alerts),
            "recent_surges": parsed_alerts[:3],
        }
    except Exception as e:
        return {
            "status": "degraded",
            "executive_insight": f"Intelligence computation temporarily impaired: {e}",
            "top_topics": [],
            "source_health": {},
            "active_anomalies_count": 0,
        }


@router.get("/topic/{topic}")
async def get_topic_dossier(
    topic: str,
    request: Request,
) -> Dict[str, Any]:
    """Retrieves deep-dive intelligence dossier for any specified entity or topic."""
    redis_client = getattr(request.app.state, "redis_client", None)
    clean_topic = normalize_topic(topic)
    topic_key = clean_topic.lower()

    if not topic_key:
        raise HTTPException(status_code=400, detail="Invalid topic query")

    if not redis_client:
        return {
            "topic": clean_topic,
            "status": "UNKNOWN",
            "total_activity": 0,
            "sources": {},
            "recent_events": [],
            "communities": [],
            "assessment": "Data store currently unreachable.",
        }

    try:
        pipe = redis_client.pipeline()
        pipe.get(topic_source_key("wikipedia", topic_key))
        pipe.get(topic_source_key("hackernews", topic_key))
        pipe.get(topic_source_key("github", topic_key))
        pipe.get(topic_source_key("reddit", topic_key))
        pipe.get(topic_source_key("devto", topic_key))
        pipe.lrange(f"ps:topic:events:{topic_key}", 0, 9)
        pipe.zscore(hot_topics_key(), clean_topic)

        results = await pipe.execute()

        wiki_count = int(results[0] or 0) if len(results) > 0 else 0
        hn_count = int(results[1] or 0) if len(results) > 1 else 0
        gh_count = int(results[2] or 0) if len(results) > 2 else 0
        reddit_count = int(results[3] or 0) if len(results) > 3 else 0
        devto_count = int(results[4] or 0) if len(results) > 4 else 0
        raw_events = results[5] if len(results) > 5 and isinstance(results[5], list) else []
        rank_score = results[6] if len(results) > 6 else None

        total_activity = wiki_count + hn_count + gh_count + reddit_count + devto_count
        if total_activity == 0 and rank_score is not None:
            total_activity = int(rank_score)

        parsed_events = []
        communities = set()
        for ev in raw_events:
            try:
                ev_data = json.loads(ev)
                parsed_events.append(ev_data)
                if ev_data.get("community_id"):
                    communities.add(ev_data["community_id"])
            except Exception:
                pass

        # Determine status
        active_sources = []
        if wiki_count > 0:
            active_sources.append(f"Wikipedia ({wiki_count})")
        if hn_count > 0:
            active_sources.append(f"Hacker News ({hn_count})")
        if gh_count > 0:
            active_sources.append(f"GitHub ({gh_count})")
        if reddit_count > 0:
            active_sources.append(f"Reddit ({reddit_count})")
        if devto_count > 0:
            active_sources.append(f"Dev.to ({devto_count})")

        if len(active_sources) >= 2 or total_activity >= 6:
            status = "SURGING"
            assessment = (
                f"🔥 High Multi-Platform Momentum: '{clean_topic}' has recorded {total_activity} events "
                f"across " + ", ".join(active_sources) + " within the rolling 30-minute window."
            )
        elif total_activity > 0:
            status = "ACTIVE"
            assessment = (
                f"Active Discussion: '{clean_topic}' has recorded {total_activity} recent event(s), "
                f"primarily active on " + ", ".join(active_sources) + "."
            )
        else:
            status = "QUIET"
            assessment = f"No elevated momentum recorded for '{clean_topic}' in the current window."

        return {
            "topic": clean_topic,
            "topic_key": topic_key,
            "status": status,
            "total_activity": total_activity,
            "sources": {
                "WIKIPEDIA": wiki_count,
                "HACKERNEWS": hn_count,
                "GITHUB": gh_count,
                "REDDIT": reddit_count,
                "DEVTO": devto_count,
            },
            "active_platforms": len(active_sources),
            "communities": sorted(list(communities)),
            "recent_events": parsed_events,
            "assessment": assessment,
        }
    except Exception as e:
        return {
            "topic": clean_topic,
            "status": "ERROR",
            "total_activity": 0,
            "sources": {},
            "recent_events": [],
            "communities": [],
            "assessment": f"Failed to compile dossier: {e}",
        }
