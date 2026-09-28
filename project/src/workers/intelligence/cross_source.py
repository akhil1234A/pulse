"""Cross-source trending intelligence detector.

Correlates activity across multiple data sources (e.g., Wikipedia + Hacker News)
to detect breaking news and real-world event surges before they hit mainstream headlines.
"""

import asyncio
import json
import logging
import re
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from src.common.models import EventSource, PulseStreamEvent
from src.common.redis_utils import (
    cross_source_alert_dedup_key,
    cross_source_alerts_key,
    cross_source_channel_key,
    hot_topics_key,
    topic_source_key,
)

logger = logging.getLogger(__name__)

# Common stopwords to ignore when extracting topics
STOPWORDS: Set[str] = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
    "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
    "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
    "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves", "show", "ask", "tell", "hn", "new", "using", "first",
}


@dataclass
class CrossSourceAlert:
    """Alert emitted when multiple data sources simultaneously surge on a topic."""

    topic: str
    wikipedia_count: int
    hackernews_count: int
    detected_at: str
    github_count: int = 0
    reddit_count: int = 0
    devto_count: int = 0
    severity: str = "HIGH"
    anomaly_type: str = "CROSS_SOURCE_CORRELATION"
    description: str = ""
    event_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if not self.description:
            active = []
            if self.wikipedia_count > 0:
                active.append(f"Wikipedia ({self.wikipedia_count} edits)")
            if self.hackernews_count > 0:
                active.append(f"Hacker News ({self.hackernews_count} posts)")
            if self.github_count > 0:
                active.append(f"GitHub ({self.github_count} repos)")
            if self.reddit_count > 0:
                active.append(f"Reddit ({self.reddit_count} posts)")
            if self.devto_count > 0:
                active.append(f"Dev.to ({self.devto_count} articles)")
            if len(active) >= 2:
                data["description"] = f"🔥 Multi-Platform Surge: '{self.topic}' active on " + ", ".join(active) + "."
            else:
                data["description"] = (
                    f"🔥 Cross-Source Surge: '{self.topic}' is active on Wikipedia "
                    f"({self.wikipedia_count} edits) and Hacker News ({self.hackernews_count} posts)."
                )
        return data


def normalize_topic(text: str) -> str:
    """Normalizes topic text for key generation."""
    # Remove parens and extra content: e.g. "OpenAI (organization)" -> "OpenAI"
    cleaned = re.sub(r"\(.*?\)", "", text).strip()
    # Replace non-alphanumeric (except hyphens and spaces) with spaces
    cleaned = re.sub(r"[^\w\s\-]", " ", cleaned)
    # Collapse consecutive whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def extract_candidate_topics(event: PulseStreamEvent) -> List[str]:
    """Extracts entity/topic candidates from an event payload based on source."""
    topics: List[str] = []
    payload = event.payload_dict
    title = payload.get("title", "").strip()

    if not title:
        return topics

    if event.source == EventSource.WIKIPEDIA:
        # Wikipedia titles are direct entity names (e.g., "DeepSeek", "Boeing 737 MAX")
        clean_title = normalize_topic(title)
        if len(clean_title) >= 3 and clean_title.lower() not in STOPWORDS:
            topics.append(clean_title)

    elif event.source in (EventSource.HACKERNEWS, EventSource.REDDIT, EventSource.DEVTO):
        # HN, Reddit, and Dev.to titles are headlines: extract capitalized entity phrases or key proper nouns
        clean_title = normalize_topic(title)

        # 1. Check for consecutive capitalized words (named entities like "Sam Altman", "DeepSeek AI")
        entity_matches = re.findall(r"\b[A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*)*\b", title)
        for entity in entity_matches:
            ent_clean = normalize_topic(entity)
            if len(ent_clean) >= 3 and ent_clean.lower() not in STOPWORDS:
                topics.append(ent_clean)

        # 2. Also consider the cleaned title if short (< 40 chars)
        if 3 <= len(clean_title) <= 40 and clean_title.lower() not in STOPWORDS:
            topics.append(clean_title)

        # 3. For Dev.to, also incorporate article tags directly (e.g. 'python', 'ai', 'rust')
        if event.source == EventSource.DEVTO:
            tags = payload.get("tags") or []
            for tag in tags:
                tag_clean = normalize_topic(str(tag))
                if len(tag_clean) >= 2 and tag_clean.lower() not in STOPWORDS:
                    topics.append(tag_clean.title() if len(tag_clean) <= 4 else tag_clean)

    elif event.source == EventSource.GITHUB:
        # GitHub events: extract repository project name and any capitalized entity phrases
        if event.community_id and "/" in event.community_id:
            repo_part = event.community_id.split("/")[-1]
            clean_repo = normalize_topic(repo_part)
            if len(clean_repo) >= 3 and clean_repo.lower() not in STOPWORDS:
                topics.append(clean_repo)

        entity_matches = re.findall(r"\b[A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*)*\b", title)
        for entity in entity_matches:
            ent_clean = normalize_topic(entity)
            if len(ent_clean) >= 3 and ent_clean.lower() not in STOPWORDS:
                topics.append(ent_clean)

    return list(dict.fromkeys(topics))  # deduplicate preserving order


class CrossSourceIntelligenceDetector:
    """Monitors activity streams across Wikipedia, Hacker News, GitHub, Reddit, and Dev.to to detect co-occurring surges."""

    def __init__(
        self,
        wiki_threshold: int = 3,
        hn_threshold: int = 2,
        window_seconds: int = 1800,  # 30-minute rolling activity window
        alert_cooldown_seconds: int = 1800,  # 30-minute alert cooldown per topic
    ) -> None:
        self.wiki_threshold = wiki_threshold
        self.hn_threshold = hn_threshold
        self.window_seconds = window_seconds
        self.alert_cooldown_seconds = alert_cooldown_seconds

    async def evaluate_event(
        self,
        event: PulseStreamEvent,
        redis_client: Any,
        db_pool: Optional[Any] = None,
        pg_circuit: Optional[Any] = None,
        redis_circuit: Optional[Any] = None,
    ) -> List[CrossSourceAlert]:
        """Evaluates a single event, tracks topic counts, and returns any triggered alerts."""
        if not redis_client or (redis_circuit and not redis_circuit.is_call_permitted()):
            return []

        topics = extract_candidate_topics(event)
        if not topics:
            return []

        alerts: List[CrossSourceAlert] = []
        source_name = event.source.value.lower()
        now_iso = datetime.now(timezone.utc).isoformat()
        payload = event.payload_dict
        title = payload.get("title", "")

        for topic in topics:
            topic_key = normalize_topic(topic).lower()
            if not topic_key or len(topic_key) < 3:
                continue

            try:
                # 1. Increment topic count for the event's source
                src_key = topic_source_key(source_name, topic_key)
                count = await redis_client.incr(src_key)
                if count == 1:
                    await redis_client.expire(src_key, self.window_seconds)

                # Index recent event sample for this topic
                event_summary = {
                    "event_id": event.event_id,
                    "source": event.source.value,
                    "event_type": event.event_type.value,
                    "community_id": event.community_id,
                    "user_id": event.user_id,
                    "title": str(title)[:120],
                    "timestamp": event.timestamp,
                }
                recent_topic_key = f"ps:topic:events:{topic_key}"
                pipe_prep = redis_client.pipeline()
                pipe_prep.lpush(recent_topic_key, json.dumps(event_summary))
                pipe_prep.ltrim(recent_topic_key, 0, 9)
                pipe_prep.expire(recent_topic_key, self.window_seconds)
                await pipe_prep.execute()

                # 2. Check counts from all data sources
                wiki_key = topic_source_key("wikipedia", topic_key)
                hn_key = topic_source_key("hackernews", topic_key)
                gh_key = topic_source_key("github", topic_key)
                reddit_key = topic_source_key("reddit", topic_key)
                devto_key = topic_source_key("devto", topic_key)

                pipe = redis_client.pipeline()
                pipe.get(wiki_key)
                pipe.get(hn_key)
                pipe.get(gh_key)
                pipe.get(reddit_key)
                pipe.get(devto_key)
                counts = await pipe.execute()

                wiki_count = int(counts[0] or 0) if len(counts) > 0 else 0
                hn_count = int(counts[1] or 0) if len(counts) > 1 else 0
                gh_count = int(counts[2] or 0) if len(counts) > 2 else 0
                reddit_count = int(counts[3] or 0) if len(counts) > 3 else 0
                devto_count = int(counts[4] or 0) if len(counts) > 4 else 0

                # 3. Update hot topics ranking in Redis sorted set
                combined_score = wiki_count + hn_count + gh_count + reddit_count + devto_count
                await redis_client.zadd(hot_topics_key(), {topic: combined_score})
                await redis_client.expire(hot_topics_key(), self.window_seconds)

                # 4. Check if multi-source surge criteria is satisfied
                is_surge = False
                if wiki_count >= self.wiki_threshold and hn_count >= self.hn_threshold:
                    is_surge = True
                elif len([c for c in (wiki_count, hn_count, gh_count, reddit_count, devto_count) if c >= 2]) >= 2:
                    is_surge = True

                if is_surge:
                    dedup_key = cross_source_alert_dedup_key(topic_key)
                    # Atomically set cooldown lock
                    was_set = await redis_client.set(
                        dedup_key, "1", ex=self.alert_cooldown_seconds, nx=True
                    )
                    if was_set:
                        # Build formatted description
                        active_parts = []
                        if wiki_count > 0:
                            active_parts.append(f"Wikipedia ({wiki_count} edits)")
                        if hn_count > 0:
                            active_parts.append(f"Hacker News ({hn_count} posts)")
                        if gh_count > 0:
                            active_parts.append(f"GitHub ({gh_count} repos)")
                        if reddit_count > 0:
                            active_parts.append(f"Reddit ({reddit_count} posts)")
                        if devto_count > 0:
                            active_parts.append(f"Dev.to ({devto_count} articles)")

                        if len(active_parts) >= 2:
                            desc = f"🔥 Multi-Platform Surge: '{topic}' is surging on " + " and ".join(active_parts) + "."
                        else:
                            desc = f"🔥 Cross-Source Surge: '{topic}' is surging on Wikipedia ({wiki_count} edits) and Hacker News ({hn_count} posts)."

                        alert = CrossSourceAlert(
                            topic=topic,
                            wikipedia_count=wiki_count,
                            hackernews_count=hn_count,
                            github_count=gh_count,
                            reddit_count=reddit_count,
                            devto_count=devto_count,
                            detected_at=now_iso,
                            severity="HIGH",
                            description=desc,
                            event_ids=[event.event_id],
                        )
                        alerts.append(alert)
                        await self._persist_and_broadcast_alert(
                            alert, redis_client, db_pool, pg_circuit
                        )

            except Exception as e:
                logger.warning("Error evaluating cross-source topic '%s': %s", topic, e)


        return alerts

    async def _persist_and_broadcast_alert(
        self,
        alert: CrossSourceAlert,
        redis_client: Any,
        db_pool: Optional[Any],
        pg_circuit: Optional[Any],
    ) -> None:
        """Persists the cross-source alert to PostgreSQL and Redis pub/sub."""
        logger.warning(
            "CROSS-SOURCE INTELLIGENCE ALERT: topic=%s (wiki=%d, hn=%d)",
            alert.topic,
            alert.wikipedia_count,
            alert.hackernews_count,
        )
        alert_dict = alert.to_dict()
        alert_json = json.dumps(alert_dict)

        # 1. Store in Redis recent alerts list and publish to Redis pub/sub channel
        try:
            pipe = redis_client.pipeline()
            pipe.lpush(cross_source_alerts_key(), alert_json)
            pipe.ltrim(cross_source_alerts_key(), 0, 49)
            pipe.publish(cross_source_channel_key(), alert_json)
            # Also push to anomalies channel so existing anomaly listeners receive it
            pipe.publish("ps:channel:anomalies", alert_json)
            await pipe.execute()
        except Exception as e:
            logger.warning("Failed to publish cross-source alert to Redis: %s", e)

        # 2. Persist to PostgreSQL anomalies table
        if db_pool and (pg_circuit is None or pg_circuit.is_call_permitted()):
            try:
                uuid_list = []
                for eid in alert.event_ids:
                    try:
                        uuid_list.append(uuid.UUID(str(eid)))
                    except ValueError:
                        pass

                details_json = json.dumps(
                    {
                        "topic": alert.topic,
                        "wikipedia_count": alert.wikipedia_count,
                        "hackernews_count": alert.hackernews_count,
                        "detected_at": alert.detected_at,
                    }
                )
                query = """
                    INSERT INTO anomalies (anomaly_type, severity, description, details, event_ids)
                    VALUES ($1, $2, $3, $4::jsonb, $5)
                    RETURNING id;
                """
                async with db_pool.acquire() as conn:
                    await conn.fetchval(
                        query,
                        alert.anomaly_type,
                        alert.severity,
                        alert.description,
                        details_json,
                        uuid_list,
                    )
                if pg_circuit:
                    pg_circuit.record_success()
            except Exception as e:
                logger.error("Failed to persist cross-source alert to database: %s", e)
                if pg_circuit:
                    pg_circuit.record_failure()
