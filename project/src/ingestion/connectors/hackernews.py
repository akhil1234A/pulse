"""Hacker News Firebase REST API connector with polling and normalization."""

import asyncio
import json
import logging
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Deque, Dict, List, Optional, Set
from urllib.parse import urlparse

import aiohttp

from src.common.models import EventSource, EventType
from src.common.retry import calculate_backoff_delay

logger = logging.getLogger(__name__)

DEFAULT_HN_BASE_URL = "https://hacker-news.firebaseio.com/v0"
USER_AGENT = (
    "PulseStream/1.0 (https://github.com/pulsestream; dev@pulsestream.local) Python-aiohttp"
)

# Mapping HN item types to PulseStream EventType
HN_EVENT_TYPE_MAP = {
    "story": EventType.POST_CREATED,
    "job": EventType.POST_CREATED,
    "poll": EventType.POST_CREATED,
    "comment": EventType.COMMENT_CREATED,
}


class HackerNewsConnector:
    """Polls the Hacker News Firebase REST API for real-time stories and updates.

    Features:
    - Polls new stories endpoint at regular intervals.
    - Concurrently fetches individual item details with concurrency limit.
    - Extracts domain as community_id (e.g., github.com, nytimes.com, or self.hackernews).
    - Normalizes HN items to PulseStream canonical event format.
    - Automatic exponential backoff on network errors.
    - Deduplicates recently processed item IDs using a bounded buffer.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_HN_BASE_URL,
        poll_interval: float = 5.0,
        max_seen_ids: int = 5000,
        initial_backfill_count: int = 15,
        concurrency: int = 5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.poll_interval = poll_interval
        self.max_seen_ids = max_seen_ids
        self.initial_backfill_count = initial_backfill_count
        self.semaphore = asyncio.Semaphore(concurrency)
        self._seen_ids: Set[int] = set()
        self._seen_queue: Deque[int] = deque()
        self._running: bool = False

    def _mark_seen(self, item_id: int) -> None:
        if item_id in self._seen_ids:
            return
        self._seen_ids.add(item_id)
        self._seen_queue.append(item_id)
        if len(self._seen_queue) > self.max_seen_ids:
            old_id = self._seen_queue.popleft()
            self._seen_ids.discard(old_id)

    def _extract_community(self, url: Optional[str]) -> str:
        if not url:
            return "self.hackernews"
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if domain.startswith("www."):
                domain = domain[4:]
            return domain if domain else "self.hackernews"
        except Exception:
            return "self.hackernews"

    def normalize_raw_event(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalizes a raw Hacker News item into the PulseStream event format."""
        if not raw_data or raw_data.get("deleted") or raw_data.get("dead"):
            return None

        hn_type = raw_data.get("type", "story")
        mapped_type = HN_EVENT_TYPE_MAP.get(hn_type, EventType.POST_CREATED)

        user_id = raw_data.get("by") or "anonymous_hn"
        item_id = raw_data.get("id")
        title = raw_data.get("title", "")
        url = raw_data.get("url", "")
        community_id = self._extract_community(url)

        # Timestamp: HN provides unix epoch in seconds
        ts_raw = raw_data.get("time")
        if isinstance(ts_raw, (int, float)):
            timestamp_ms = int(ts_raw * 1000)
        else:
            timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        payload = {
            "title": title,
            "url": url,
            "score": raw_data.get("score", 0),
            "hn_id": item_id,
            "hn_type": hn_type,
            "descendants": raw_data.get("descendants", 0),
            "text": raw_data.get("text", "")[:500] if raw_data.get("text") else "",
        }

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": mapped_type.value,
            "source": EventSource.HACKERNEWS.value,
            "timestamp": timestamp_ms,
            "user_id": str(user_id),
            "community_id": str(community_id),
            "payload": json.dumps(payload),
            "ingested_at": now_ms,
            "schema_version": "1.0",
        }

    async def _fetch_item(
        self, session: aiohttp.ClientSession, item_id: int
    ) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/item/{item_id}.json"
        try:
            async with self.semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10.0)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    logger.debug("Failed to fetch HN item %d (status %d)", item_id, resp.status)
                    return None
        except Exception as exc:
            logger.debug("Error fetching HN item %d: %s", item_id, exc)
            return None

    async def _fetch_new_story_ids(
        self, session: aiohttp.ClientSession
    ) -> List[int]:
        url = f"{self.base_url}/newstories.json"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10.0)) as resp:
            if resp.status != 200:
                raise aiohttp.ClientError(f"HN API returned HTTP {resp.status}")
            data = await resp.json()
            if isinstance(data, list):
                return [int(x) for x in data if isinstance(x, (int, str)) and str(x).isdigit()]
            return []

    async def stream_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Polls Hacker News API for new items, yielding normalized event dicts."""
        self._running = True
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        }
        attempt = 0
        first_poll = True

        logger.info("Starting Hacker News connector against %s...", self.base_url)

        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10.0, sock_read=30.0)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            while self._running:
                try:
                    story_ids = await self._fetch_new_story_ids(session)
                    attempt = 0  # Reset reconnect delay on success

                    if first_poll:
                        # On startup, only process the latest N items to avoid initial flood
                        to_process = story_ids[: self.initial_backfill_count]
                        # Mark the rest of the batch as seen
                        for sid in story_ids:
                            self._mark_seen(sid)
                        first_poll = False
                    else:
                        to_process = [sid for sid in story_ids if sid not in self._seen_ids]
                        for sid in to_process:
                            self._mark_seen(sid)

                    if to_process:
                        logger.debug("Fetching details for %d new HN items...", len(to_process))
                        fetch_tasks = [self._fetch_item(session, sid) for sid in to_process]
                        items = await asyncio.gather(*fetch_tasks, return_exceptions=True)

                        for item in items:
                            if not self._running:
                                break
                            if isinstance(item, dict):
                                normalized = self.normalize_raw_event(item)
                                if normalized:
                                    yield normalized

                    await asyncio.sleep(self.poll_interval)

                except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as exc:
                    if not self._running:
                        break
                    attempt += 1
                    delay = calculate_backoff_delay(attempt, base_ms=1000.0, max_ms=30000.0)
                    logger.warning(
                        "Hacker News API connection lost: %s. Reconnecting in %.2fs (attempt %d)...",
                        exc,
                        delay,
                        attempt,
                    )
                    await asyncio.sleep(delay)

                except Exception as e:
                    if not self._running:
                        break
                    logger.exception("Unexpected error in Hacker News connector: %s", e)
                    await asyncio.sleep(2.0)

        logger.info("HackerNewsConnector stream ended.")

    def stop(self) -> None:
        """Stops the polling loop."""
        self._running = False
