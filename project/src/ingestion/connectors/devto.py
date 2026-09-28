"""Dev.to Community Articles connector with polling and canonical normalization."""

import asyncio
import json
import logging
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Deque, Dict, List, Optional, Set

import aiohttp

from src.common.models import EventSource, EventType
from src.common.retry import calculate_backoff_delay

logger = logging.getLogger(__name__)

DEFAULT_DEVTO_API_URL = "https://dev.to/api/articles"
USER_AGENT = "PulseStream/1.0 (https://github.com/pulsestream; dev@pulsestream.local) Python-aiohttp"


class DevtoConnector:
    """Polls the Dev.to public REST API for newly published community articles & tech discussions.

    Features:
    - Zero authentication required (100% public developer community feed).
    - Polls `https://dev.to/api/articles?per_page=25` at configurable intervals.
    - Normalizes Dev.to articles into canonical PulseStream events.
    - Tags become community identifiers (e.g., 'devto/python', 'devto/ai', 'devto/webdev').
    - Maintains deduplication buffer for seen article IDs.
    - Handles network interruptions and HTTP rate limits with exponential backoff.
    """

    def __init__(
        self,
        api_url: str = DEFAULT_DEVTO_API_URL,
        poll_interval: float = 15.0,
        per_page: int = 25,
        tag: Optional[str] = None,
        max_seen_ids: int = 5000,
        initial_backfill_count: int = 25,
    ) -> None:
        self.api_url = api_url
        self.poll_interval = poll_interval
        self.per_page = per_page
        self.tag = tag
        self.max_seen_ids = max_seen_ids
        self.initial_backfill_count = initial_backfill_count
        self._seen_ids: Deque[int] = deque(maxlen=max_seen_ids)
        self._seen_ids_set: Set[int] = set()
        self._running = False
        self._is_first_poll = True

    def _mark_seen(self, item_id: int) -> None:
        if item_id in self._seen_ids_set:
            return
        if len(self._seen_ids) >= self.max_seen_ids:
            oldest = self._seen_ids.popleft()
            self._seen_ids_set.discard(oldest)
        self._seen_ids.append(item_id)
        self._seen_ids_set.add(item_id)

    def _is_seen(self, item_id: int) -> bool:
        return item_id in self._seen_ids_set

    def normalize_raw_event(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transforms a raw Dev.to article into canonical PulseStream schema."""
        article_id = article.get("id")
        title = (article.get("title") or "").strip()
        if not article_id or not title:
            return None

        # Parse published_at timestamp
        pub_str = article.get("published_at") or article.get("created_at") or ""
        ts_millis = None
        if pub_str:
            try:
                dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                ts_millis = int(dt.timestamp() * 1000)
            except Exception:
                pass

        now_millis = int(datetime.now(timezone.utc).timestamp() * 1000)
        event_timestamp = ts_millis if ts_millis else now_millis

        # Community tagging: devto/{first_tag}
        tag_list = article.get("tag_list") or []
        if isinstance(tag_list, str):
            tag_list = [t.strip() for t in tag_list.split(",") if t.strip()]
        primary_tag = tag_list[0] if tag_list else "tech"
        community_id = f"devto/{primary_tag}"

        user_info = article.get("user") or {}
        user_id = user_info.get("username") or "devto_user"

        deterministic_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"https://dev.to/articles/{article_id}"))

        payload_obj = {
            "title": title,
            "url": article.get("url", f"https://dev.to/articles/{article_id}"),
            "description": (article.get("description") or "").strip(),
            "tags": tag_list,
            "reactions": article.get("public_reactions_count") or article.get("positive_reactions_count") or 0,
            "comments_count": article.get("comments_count") or 0,
            "reading_time_minutes": article.get("reading_time_minutes") or 1,
            "author_name": user_info.get("name") or user_id,
            "author_username": user_id,
            "devto_id": article_id,
        }

        return {
            "event_id": deterministic_uuid,
            "event_type": EventType.POST_CREATED.value,
            "source": EventSource.DEVTO.value,
            "timestamp": event_timestamp,
            "user_id": user_id,
            "community_id": community_id,
            "payload": json.dumps(payload_obj),
            "ingested_at": now_millis,
            "schema_version": "1.0",
        }

    async def _fetch_articles(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        """Fetches the latest articles from Dev.to."""
        params: Dict[str, Any] = {"per_page": self.per_page}
        if self.tag:
            params["tag"] = self.tag

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/vnd.forem.api-v1+json, application/json",
        }

        async with session.get(self.api_url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 429:
                retry_after = resp.headers.get("Retry-After", "30")
                logger.warning("Dev.to rate limit encountered (429). Retry-After: %s s", retry_after)
                await asyncio.sleep(float(retry_after) if retry_after.isdigit() else 30.0)
                return []
            if resp.status != 200:
                body = await resp.text()
                logger.warning("Dev.to API error %d: %s", resp.status, body[:200])
                return []

            data = await resp.json()
            if isinstance(data, list):
                return data
            return []

    async def stream_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Continuously polls Dev.to and yields canonical events."""
        self._running = True
        consecutive_errors = 0

        logger.info(
            "Starting Dev.to connector polling (interval=%.1fs, per_page=%d, tag=%s)",
            self.poll_interval,
            self.per_page,
            self.tag or "all",
        )

        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    articles = await self._fetch_articles(session)
                    consecutive_errors = 0

                    if self._is_first_poll:
                        # Yield initial batch
                        batch = articles[: self.initial_backfill_count]
                        logger.info("Dev.to initial poll returned %d articles. Backfilling %d.", len(articles), len(batch))
                        # Yield in chronological order (oldest to newest in the batch)
                        for article in reversed(batch):
                            art_id = article.get("id")
                            if art_id:
                                self._mark_seen(art_id)
                            norm = self.normalize_raw_event(article)
                            if norm:
                                yield norm
                        self._is_first_poll = False
                    else:
                        new_articles = [art for art in articles if art.get("id") and not self._is_seen(art["id"])]
                        if new_articles:
                            logger.info("Dev.to retrieved %d new articles", len(new_articles))
                            for article in reversed(new_articles):
                                self._mark_seen(article["id"])
                                norm = self.normalize_raw_event(article)
                                if norm:
                                    yield norm

                except asyncio.CancelledError:
                    logger.info("Dev.to connector stream cancelled")
                    break
                except Exception as e:
                    consecutive_errors += 1
                    backoff = calculate_backoff_delay(consecutive_errors)
                    logger.error(
                        "Error polling Dev.to API: %s. Backing off for %.2fs (attempt %d)",
                        str(e),
                        backoff,
                        consecutive_errors,
                    )
                    await asyncio.sleep(backoff)

                await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        """Stops the streaming loop."""
        self._running = False
