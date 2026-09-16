"""Pluggable anomaly detection rules for PulseStream."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import json
import time
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis

from src.common.logging import get_logger
from src.common.models import PulseStreamEvent
from src.common.redis_utils import anomaly_community_key, anomaly_user_key

logger = get_logger(__name__)


@dataclass
class AnomalyRecord:
    """Represents a detected anomaly record to be recorded in DB and Redis."""

    anomaly_type: str
    severity: str
    description: str
    details: Dict[str, Any] = field(default_factory=dict)
    event_ids: List[str] = field(default_factory=list)


class BaseAnomalyRule(ABC):
    """Abstract base class for individual anomaly detection rules."""

    rule_name: str = "base_rule"

    @abstractmethod
    async def evaluate_event(
        self,
        event: PulseStreamEvent,
        redis_client: Optional[aioredis.Redis],
    ) -> Optional[AnomalyRecord]:
        """Evaluates an individual event against the rule."""
        pass


class UserFloodRule(BaseAnomalyRule):
    """Detects when a single user emits more than threshold (default 50) events in 60 seconds."""

    rule_name = "USER_FLOOD"

    def __init__(self, threshold: int = 50, window_seconds: int = 60) -> None:
        self.threshold = threshold
        self.window_seconds = window_seconds

    async def evaluate_event(
        self,
        event: PulseStreamEvent,
        redis_client: Optional[aioredis.Redis],
    ) -> Optional[AnomalyRecord]:
        if not redis_client or not event.user_id:
            return None

        key = anomaly_user_key(event.user_id)
        now_ts = int(time.time())
        cutoff_ts = now_ts - self.window_seconds

        try:
            member = f"{event.event_id}:{now_ts}"
            async with redis_client.pipeline(transaction=True) as pipe:
                pipe.zadd(key, {member: now_ts})
                pipe.zremrangebyscore(key, 0, cutoff_ts)
                pipe.zcard(key)
                pipe.expire(key, 300)  # 5 min TTL
                results = await pipe.execute()

            event_count = results[2]
            if event_count > self.threshold:
                return AnomalyRecord(
                    anomaly_type="USER_FLOOD",
                    severity="HIGH",
                    description=(
                        f"User {event.user_id} triggered flood alert: "
                        f"{event_count} events in last {self.window_seconds}s (limit {self.threshold})"
                    ),
                    details={
                        "user_id": event.user_id,
                        "count": event_count,
                        "window_seconds": self.window_seconds,
                        "threshold": self.threshold,
                    },
                    event_ids=[event.event_id],
                )
        except Exception as e:
            logger.warning("Error evaluating UserFloodRule: %s", e)

        return None


class CommunitySpikeRule(BaseAnomalyRule):
    """Detects when a community event rate spikes >10x compared to its 1-hour rolling average."""

    rule_name = "COMMUNITY_SPIKE"

    def __init__(self, multiplier_threshold: float = 10.0, min_events: int = 10) -> None:
        self.multiplier_threshold = multiplier_threshold
        self.min_events = min_events

    async def evaluate_event(
        self,
        event: PulseStreamEvent,
        redis_client: Optional[aioredis.Redis],
    ) -> Optional[AnomalyRecord]:
        if not redis_client or not event.community_id:
            return None

        key = anomaly_community_key(event.community_id)
        now_ts = int(time.time())
        one_min_ago = now_ts - 60
        one_hour_ago = now_ts - 3600

        try:
            member = f"{event.event_id}:{now_ts}"
            async with redis_client.pipeline(transaction=True) as pipe:
                pipe.zadd(key, {member: now_ts})
                pipe.zremrangebyscore(key, 0, one_hour_ago)
                # Count in last 1 minute
                pipe.zcount(key, one_min_ago, now_ts)
                # Count in last 1 hour
                pipe.zcard(key)
                pipe.expire(key, 3600)
                results = await pipe.execute()

            count_1min = results[2]
            count_1hour = results[3]

            if count_1min >= self.min_events and count_1hour > 0:
                # Rolling 1-hour average per minute: total / 60 minutes
                avg_per_minute = max(1.0, count_1hour / 60.0)
                if count_1min > (avg_per_minute * self.multiplier_threshold):
                    return AnomalyRecord(
                        anomaly_type="COMMUNITY_SPIKE",
                        severity="MEDIUM",
                        description=(
                            f"Community {event.community_id} activity spike: {count_1min} events/min, "
                            f"exceeding {self.multiplier_threshold}x rolling average ({avg_per_minute:.1f}/min)"
                        ),
                        details={
                            "community_id": event.community_id,
                            "count_1min": count_1min,
                            "avg_per_minute": avg_per_minute,
                            "count_1hour": count_1hour,
                            "multiplier": count_1min / avg_per_minute,
                        },
                        event_ids=[event.event_id],
                    )
        except Exception as e:
            logger.warning("Error evaluating CommunitySpikeRule: %s", e)

        return None


class DuplicateStormRule:
    """Batch-level rule checking for duplicate bursts exceeding a threshold ratio (e.g. >5%)."""

    rule_name = "DUPLICATE_STORM"

    def __init__(self, threshold_ratio: float = 0.05, min_batch_size: int = 10) -> None:
        self.threshold_ratio = threshold_ratio
        self.min_batch_size = min_batch_size

    def evaluate_batch(
        self,
        duplicate_count: int,
        total_count: int,
        duplicate_event_ids: List[str],
    ) -> Optional[AnomalyRecord]:
        if total_count < self.min_batch_size or duplicate_count == 0:
            return None

        ratio = duplicate_count / float(total_count)
        if ratio > self.threshold_ratio:
            return AnomalyRecord(
                anomaly_type="DUPLICATE_STORM",
                severity="HIGH",
                description=(
                    f"Duplicate storm detected: {duplicate_count}/{total_count} ({ratio:.1%}) "
                    f"duplicates in polled batch exceeding {self.threshold_ratio:.1%} threshold"
                ),
                details={
                    "duplicate_count": duplicate_count,
                    "total_count": total_count,
                    "duplicate_ratio": ratio,
                    "threshold_ratio": self.threshold_ratio,
                },
                event_ids=duplicate_event_ids[:20],  # sample up to 20 IDs
            )
        return None
