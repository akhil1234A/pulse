"""Reddit public feed connector with OAuth2 support, Atom RSS/XML & JSON polling, and canonical normalization."""

import asyncio
import json
import logging
import os
import time
import uuid
import xml.etree.ElementTree as ET
from collections import deque
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Deque, Dict, List, Optional, Set

import aiohttp

from src.common.models import EventSource, EventType
from src.common.retry import calculate_backoff_delay

logger = logging.getLogger(__name__)

DEFAULT_REDDIT_FEED_URL = "https://old.reddit.com/r/technology/new.rss"
OAUTH_REDDIT_API_URL = "https://oauth.reddit.com/r/technology+programming+artificial/new"
USER_AGENT = "PulseStream/1.0 (by /u/pulsestream_aggregator; dev@pulsestream.local)"
BROWSER_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def _parse_atom_xml(xml_text: str) -> List[Dict[str, Any]]:
    """Parses Reddit's Atom RSS XML feed into structured post objects."""
    posts: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns):
            title_elem = entry.find("atom:title", ns)
            title = title_elem.text if title_elem is not None and title_elem.text else ""
            if not title:
                continue

            id_elem = entry.find("atom:id", ns)
            post_id = id_elem.text if id_elem is not None and id_elem.text else ""

            link_elem = entry.find("atom:link", ns)
            post_url = link_elem.attrib.get("href", "") if link_elem is not None else ""

            author_elem = entry.find("atom:author/atom:name", ns)
            author = author_elem.text if author_elem is not None and author_elem.text else "anonymous_reddit"
            if author.startswith("/u/"):
                author = author[3:]

            pub_elem = entry.find("atom:published", ns)
            pub_date = pub_elem.text if pub_elem is not None and pub_elem.text else ""

            cat_elem = entry.find("atom:category", ns)
            subreddit = "technology"
            if cat_elem is not None:
                sub_label = cat_elem.attrib.get("label") or cat_elem.attrib.get("term") or ""
                if sub_label.startswith("r/"):
                    subreddit = sub_label[2:]
                elif sub_label:
                    subreddit = sub_label

            posts.append({
                "id": post_id,
                "title": title,
                "author": author,
                "subreddit": subreddit,
                "url": post_url,
                "published": pub_date,
            })
    except Exception as e:
        logger.warning("Failed to parse Reddit Atom XML: %s", e)
    return posts


class RedditConnector:
    """Polls public Reddit feeds (OAuth2 JSON or Atom RSS XML) for real-time community discussions.

    Features:
    - If `client_id` and `client_secret` (or REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET) are provided,
      uses official Reddit OAuth2 to query `oauth.reddit.com` without IP blocks.
    - If no credentials are provided, safely polls public RSS/Atom feeds from `old.reddit.com`.
    - Normalizes Reddit submissions to PulseStream canonical event format.
    - Sets community_id to `r/{subreddit}` (e.g. `r/technology`, `r/programming`).
    - Maintains deduplication buffer for post IDs across polling cycles.
    """

    def __init__(
        self,
        feed_url: str = DEFAULT_REDDIT_FEED_URL,
        poll_interval: float = 20.0,
        max_seen_ids: int = 5000,
        initial_backfill_count: int = 15,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ) -> None:
        self.feed_url = feed_url
        self.poll_interval = poll_interval
        self.max_seen_ids = max_seen_ids
        self.initial_backfill_count = initial_backfill_count
        self.client_id = client_id or os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("REDDIT_CLIENT_SECRET")
        self._seen_ids: Set[str] = set()
        self._seen_queue: Deque[str] = deque()
        self._running: bool = False
        self._access_token: Optional[str] = None
        self._token_expiry: float = 0.0

    def _mark_seen(self, item_id: str) -> None:
        if item_id in self._seen_ids:
            return
        self._seen_ids.add(item_id)
        self._seen_queue.append(item_id)
        if len(self._seen_queue) > self.max_seen_ids:
            old_id = self._seen_queue.popleft()
            self._seen_ids.discard(old_id)

    async def _get_oauth_token(self, session: aiohttp.ClientSession) -> Optional[str]:
        """Obtains an OAuth2 bearer token from Reddit using client_credentials grant."""
        if not (self.client_id and self.client_secret):
            return None

        now = time.time()
        if self._access_token and now < (self._token_expiry - 60):
            return self._access_token

        auth = aiohttp.BasicAuth(self.client_id, self.client_secret)
        headers = {"User-Agent": USER_AGENT}
        data = {"grant_type": "client_credentials"}
        try:
            async with session.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=auth,
                data=data,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10.0),
            ) as resp:
                if resp.status == 200:
                    token_data = await resp.json()
                    self._access_token = token_data.get("access_token")
                    expires_in = token_data.get("expires_in", 3600)
                    self._token_expiry = now + expires_in
                    logger.info("Reddit OAuth token obtained successfully (expires in %ds).", expires_in)
                    return self._access_token
                logger.warning("Reddit OAuth token request failed with status HTTP %d.", resp.status)
        except Exception as e:
            logger.warning("Error acquiring Reddit OAuth token: %s", e)
        return None

    def normalize_raw_event(self, raw_post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalizes a raw Reddit submission (Atom dict or JSON dict) into PulseStream format."""
        if not raw_post or not isinstance(raw_post, dict):
            return None

        data = raw_post.get("data", raw_post)
        post_id = str(data.get("id") or "")
        title = str(data.get("title") or "").strip()
        if not title:
            return None

        author = data.get("author") or "anonymous_reddit"
        if str(author).startswith("/u/"):
            author = str(author)[3:]

        subreddit = data.get("subreddit") or "technology"
        community_id = f"r/{subreddit.lower()}"

        # Timestamp extraction
        if "published" in data and data["published"]:
            try:
                dt = datetime.fromisoformat(data["published"])
                timestamp_ms = int(dt.timestamp() * 1000)
            except Exception:
                timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        else:
            created_utc = data.get("created_utc")
            if isinstance(created_utc, (int, float)):
                timestamp_ms = int(created_utc * 1000)
            else:
                timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        permalink = data.get("permalink", "")
        post_url = f"https://reddit.com{permalink}" if permalink else (data.get("url") or "")

        payload = {
            "title": title,
            "url": post_url,
            "external_url": data.get("url", ""),
            "subreddit": subreddit,
            "score": data.get("score", 1),
            "num_comments": data.get("num_comments", 0),
            "reddit_id": post_id,
            "author": str(author),
            "flair": data.get("link_flair_text") or "",
            "selftext": (data.get("selftext") or "")[:400],
        }

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": EventType.POST_CREATED.value,
            "source": EventSource.REDDIT.value,
            "timestamp": timestamp_ms,
            "user_id": str(author),
            "community_id": str(community_id),
            "payload": json.dumps(payload),
            "ingested_at": now_ms,
            "schema_version": "1.0",
        }

    async def _fetch_posts(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        """Fetches latest submissions using OAuth if available, otherwise RSS."""
        # 1. Try OAuth if configured
        token = await self._get_oauth_token(session)
        if token:
            headers = {
                "Authorization": f"Bearer {token}",
                "User-Agent": USER_AGENT,
            }
            try:
                async with session.get(
                    OAUTH_REDDIT_API_URL,
                    headers=headers,
                    params={"limit": 25},
                    timeout=aiohttp.ClientTimeout(total=10.0),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        children = data.get("data", {}).get("children", [])
                        if isinstance(children, list):
                            return children
                    logger.debug("OAuth Reddit request returned HTTP %d, falling back.", resp.status)
            except Exception as e:
                logger.debug("OAuth Reddit request error: %s", e)

        # 2. Fallback to public RSS/Atom XML
        headers = {
            "User-Agent": BROWSER_USER_AGENT,
            "Accept": "application/atom+xml,application/xml,text/xml,*/*",
        }
        async with session.get(
            self.feed_url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10.0),
        ) as resp:
            if resp.status == 429:
                logger.warning("Reddit API rate limited (HTTP 429). Pausing.")
                raise aiohttp.ClientResponseError(
                    resp.request_info,
                    resp.history,
                    status=resp.status,
                    message="Rate limit",
                )
            if resp.status != 200:
                raise aiohttp.ClientError(f"Reddit feed returned HTTP {resp.status}")

            content_type = str(resp.headers.get("Content-Type", "")) if hasattr(resp, "headers") else ""
            text = ""
            if hasattr(resp, "text"):
                try:
                    res = await resp.text()
                    if isinstance(res, str):
                        text = res
                except Exception:
                    text = ""

            # If Atom XML / RSS
            if isinstance(text, str) and text and ("xml" in content_type or text.strip().startswith("<?xml") or "<feed" in text):
                parsed = _parse_atom_xml(text)
                if parsed:
                    return parsed

            # If JSON
            try:
                if hasattr(resp, "json"):
                    data = await resp.json()
                elif text:
                    data = json.loads(text)
                else:
                    data = {}
                if isinstance(data, dict):
                    children = data.get("data", {}).get("children", [])
                    if isinstance(children, list):
                        return children
            except Exception:
                pass

            return []

    async def stream_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Polls Reddit feed for new submissions, yielding normalized events."""
        self._running = True
        attempt = 0
        first_poll = True

        mode = "OAuth2" if (self.client_id and self.client_secret) else "Public RSS"
        logger.info("Starting Reddit connector (%s mode)...", mode)

        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10.0, sock_read=30.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while self._running:
                try:
                    posts = await self._fetch_posts(session)
                    attempt = 0

                    new_posts: List[Dict[str, Any]] = []
                    for item in posts:
                        pid = str(item.get("data", {}).get("id") if "data" in item else item.get("id") or "")
                        if not pid:
                            continue
                        if first_poll:
                            if len(new_posts) < self.initial_backfill_count:
                                new_posts.append(item)
                            self._mark_seen(pid)
                        else:
                            if pid not in self._seen_ids:
                                self._mark_seen(pid)
                                new_posts.append(item)

                    if first_poll:
                        first_poll = False

                    for item in new_posts:
                        if not self._running:
                            break
                        normalized = self.normalize_raw_event(item)
                        if normalized:
                            yield normalized

                    await asyncio.sleep(self.poll_interval)

                except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as exc:
                    if not self._running:
                        break
                    attempt += 1
                    delay = calculate_backoff_delay(attempt, base_ms=5000.0, max_ms=60000.0)
                    logger.warning(
                        "Reddit feed issue: %s. Retrying in %.2fs (attempt %d)...",
                        exc,
                        delay,
                        attempt,
                    )
                    await asyncio.sleep(delay)

                except Exception as e:
                    if not self._running:
                        break
                    logger.exception("Unexpected error in Reddit connector: %s", e)
                    await asyncio.sleep(5.0)

        logger.info("RedditConnector stream ended.")

    def stop(self) -> None:
        """Stops the polling loop."""
        self._running = False
