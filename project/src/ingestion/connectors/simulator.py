"""High-throughput synthetic event simulator with Zipf skew and chaos injection modes."""

import asyncio
from collections import deque
from datetime import datetime, timezone
import json
import logging
import random
import time
from typing import Any, AsyncGenerator, Deque, Dict, List, Optional
import uuid

from src.common.models import EventSource, EventType

logger = logging.getLogger(__name__)

COMMUNITIES = [
    "technology",
    "programming",
    "news",
    "science",
    "gaming",
    "music",
    "movies",
    "books",
    "art",
    "crypto",
    "sports",
    "travel",
    "food",
    "finance",
    "photography",
    "design",
    "history",
    "philosophy",
    "fitness",
    "diy",
]

EVENT_TYPE_WEIGHTS = [
    (EventType.POST_CREATED, 0.40),
    (EventType.COMMENT_CREATED, 0.35),
    (EventType.REACTION_ADDED, 0.20),
    (EventType.USER_JOINED, 0.05),
]


class EventSimulator:
    """Simulates realistic community events at controlled throughput rates.

    Supports:
    - Zipf distribution for realistic community and user popularity skew.
    - Multiple operational modes: steady, burst, chaos, and ramp.
    - Chaos features: duplicate injection, malformed event payloads, and out-of-order timestamps.
    """

    def __init__(
        self,
        mode: str = "steady",
        rate: int = 50,
        burst_multiplier: float = 10.0,
        burst_duration_seconds: float = 30.0,
        burst_interval_seconds: float = 120.0,
        chaos_duplicate_pct: float = 0.05,
        chaos_malformed_pct: float = 0.02,
        num_users: int = 1000,
        zipf_alpha: float = 1.3,
    ) -> None:
        self.mode = mode.lower()
        self.target_rate = max(1, rate)
        self.burst_multiplier = burst_multiplier
        self.burst_duration_seconds = burst_duration_seconds
        self.burst_interval_seconds = burst_interval_seconds
        self.chaos_duplicate_pct = chaos_duplicate_pct
        self.chaos_malformed_pct = chaos_malformed_pct
        self.num_users = num_users
        self.zipf_alpha = zipf_alpha

        self._running: bool = False
        self._recent_events: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._start_time: float = 0.0
        self._events_produced: int = 0

        # Precompute Zipf probabilities for communities and users
        self._user_pool: List[str] = [f"usr_{i:04d}" for i in range(1, self.num_users + 1)]
        self._comm_weights = self._compute_zipf_weights(len(COMMUNITIES), self.zipf_alpha)
        self._user_weights = self._compute_zipf_weights(self.num_users, self.zipf_alpha)

    @staticmethod
    def _compute_zipf_weights(n: int, alpha: float) -> List[float]:
        """Calculates unnormalized or normalized Zipfian weights for rank 1..n."""
        return [1.0 / (i ** alpha) for i in range(1, n + 1)]

    def _sample_community(self) -> str:
        return random.choices(COMMUNITIES, weights=self._comm_weights, k=1)[0]

    def _sample_user(self) -> str:
        return random.choices(self._user_pool, weights=self._user_weights, k=1)[0]

    def _sample_event_type(self) -> EventType:
        types, weights = zip(*EVENT_TYPE_WEIGHTS)
        return random.choices(types, weights=weights, k=1)[0]

    def _get_current_target_rate(self, elapsed: float) -> float:
        """Determines the effective events/sec rate based on operational mode."""
        if self.mode == "steady":
            return float(self.target_rate)

        if self.mode == "burst":
            cycle_pos = elapsed % (self.burst_interval_seconds + self.burst_duration_seconds)
            if cycle_pos < self.burst_duration_seconds:
                return float(self.target_rate * self.burst_multiplier)
            return float(self.target_rate)

        if self.mode == "ramp":
            # Linear ramp from 10% to 100% over 60 seconds, then steady
            ramp_period = 60.0
            factor = min(1.0, 0.1 + 0.9 * (elapsed / ramp_period))
            return float(self.target_rate * factor)

        if self.mode == "chaos":
            return float(self.target_rate)

        return float(self.target_rate)

    def generate_single_event(self) -> Dict[str, Any]:
        """Generates a single synthetic event dictionary."""
        is_chaos = self.mode == "chaos"

        # 1. Chaos injection: duplicate event
        if is_chaos and self._recent_events and random.random() < self.chaos_duplicate_pct:
            duplicate = random.choice(self._recent_events).copy()
            logger.debug("Simulator injected duplicate event: %s", duplicate.get("event_id"))
            return duplicate

        # 2. Chaos injection: malformed event
        if is_chaos and random.random() < self.chaos_malformed_pct:
            malformed_type = random.choice(["missing_field", "invalid_enum", "bad_timestamp"])
            logger.debug("Simulator injected malformed event of type %s", malformed_type)
            if malformed_type == "missing_field":
                return {
                    "event_id": str(uuid.uuid4()),
                    "source": "SIMULATOR",
                    # Missing event_type and user_id
                    "timestamp": int(time.time() * 1000),
                }
            elif malformed_type == "invalid_enum":
                return {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "UNKNOWN_ACTION_XYZ",
                    "source": "SIMULATOR",
                    "timestamp": int(time.time() * 1000),
                    "user_id": self._sample_user(),
                    "payload": "{}",
                }
            else:
                return {
                    "event_id": str(uuid.uuid4()),
                    "event_type": EventType.POST_CREATED.value,
                    "source": "SIMULATOR",
                    "timestamp": "not-a-valid-timestamp-string",
                    "user_id": self._sample_user(),
                    "payload": "{}",
                }

        # 3. Standard valid event generation
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Out-of-order timestamp injection in chaos mode
        if is_chaos and random.random() < 0.05:
            # Shift back by 1-5 minutes
            skew_ms = random.randint(60_000, 300_000)
            event_time_ms = now_ms - skew_ms
        else:
            event_time_ms = now_ms

        event_type = self._sample_event_type()
        user_id = self._sample_user()
        community_id = None if event_type == EventType.USER_JOINED else self._sample_community()

        payload = {
            "title": f"Update in #{community_id}" if community_id else "Welcome to PulseStream",
            "score": random.randint(0, 100),
            "sim_batch": self._events_produced // 1000,
        }

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type.value,
            "source": EventSource.SIMULATOR.value,
            "timestamp": event_time_ms,
            "user_id": user_id,
            "community_id": community_id,
            "payload": json.dumps(payload),
            "ingested_at": now_ms,
            "schema_version": "1.0",
        }

        self._recent_events.append(event)
        return event

    async def stream_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Asynchronous generator yielding synthetic events at the specified rate."""
        self._running = True
        self._start_time = time.monotonic()
        logger.info(
            "EventSimulator started (mode=%s, target_rate=%d/sec)",
            self.mode,
            self.target_rate,
        )

        try:
            while self._running:
                now = time.monotonic()
                elapsed = now - self._start_time
                current_rate = self._get_current_target_rate(elapsed)

                # Time per event
                interval = 1.0 / current_rate if current_rate > 0 else 0.02
                event = self.generate_single_event()
                self._events_produced += 1

                yield event
                await asyncio.sleep(interval)
        finally:
            self._running = False
            logger.info(
                "EventSimulator stopped after producing %d events.",
                self._events_produced,
            )

    def stop(self) -> None:
        """Signals the generator to stop streaming."""
        self._running = False
