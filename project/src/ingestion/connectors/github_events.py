"""GitHub Public Events API connector with polling and canonical normalization."""

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

DEFAULT_GITHUB_API_URL = "https://api.github.com/events"
USER_AGENT = "PulseStream/1.0 (https://github.com/pulsestream; dev@pulsestream.local) Python-aiohttp"

# Mapping GitHub event types to PulseStream EventType
GH_EVENT_TYPE_MAP = {
    "PushEvent": EventType.POST_CREATED,
    "ReleaseEvent": EventType.POST_CREATED,
    "IssuesEvent": EventType.COMMENT_CREATED,
    "IssueCommentEvent": EventType.COMMENT_CREATED,
    "PullRequestEvent": EventType.COMMENT_CREATED,
    "PullRequestReviewCommentEvent": EventType.COMMENT_CREATED,
    "CommitCommentEvent": EventType.COMMENT_CREATED,
    "WatchEvent": EventType.REACTION_ADDED,
    "ForkEvent": EventType.USER_JOINED,
    "CreateEvent": EventType.USER_JOINED,
    "MemberEvent": EventType.USER_JOINED,
}


def _format_gh_title(gh_type: str, repo_name: str, payload: Dict[str, Any]) -> str:
    """Generates an informative human-readable title from a GitHub event."""
    action = payload.get("action", "")
    if gh_type == "WatchEvent":
        return f"Starred {repo_name}"
    elif gh_type == "ForkEvent":
        forkee = payload.get("forkee", {}).get("full_name", "")
        return f"Forked {repo_name} -> {forkee}" if forkee else f"Forked {repo_name}"
    elif gh_type == "PushEvent":
        commit_count = len(payload.get("commits", [])) or payload.get("size", 1)
        ref = payload.get("ref", "").replace("refs/heads/", "")
        ref_str = f" on {ref}" if ref else ""
        return f"Pushed {commit_count} commit(s) to {repo_name}{ref_str}"
    elif gh_type == "ReleaseEvent":
        tag = payload.get("release", {}).get("tag_name", "")
        tag_str = f" {tag}" if tag else ""
        return f"New release{tag_str} in {repo_name}"
    elif gh_type == "IssuesEvent":
        action_str = f" {action}" if action else ""
        issue_title = payload.get("issue", {}).get("title", "")
        return f"Issue{action_str} in {repo_name}: {issue_title}"[:120]
    elif gh_type == "PullRequestEvent":
        action_str = f" {action}" if action else ""
        pr_title = payload.get("pull_request", {}).get("title", "")
        return f"PR{action_str} in {repo_name}: {pr_title}"[:120]
    elif gh_type == "CreateEvent":
        ref_type = payload.get("ref_type", "repository")
        ref = payload.get("ref", "")
        ref_str = f" '{ref}'" if ref else ""
        return f"Created {ref_type}{ref_str} in {repo_name}"
    return f"{gh_type} on {repo_name}"


class GitHubEventsConnector:
    """Polls the GitHub Public Events API for real-time repository activities.

    Features:
    - Polls `https://api.github.com/events` at configured intervals.
    - Normalizes GitHub event items to PulseStream canonical event format.
    - Tracks repo full_name as community_id (e.g., 'facebook/react', 'torvalds/linux').
    - Maintains deduplication buffer for seen event IDs.
    - Gracefully handles GitHub API rate limits (X-RateLimit-Remaining) and backoffs.
    """

    def __init__(
        self,
        api_url: str = DEFAULT_GITHUB_API_URL,
        poll_interval: float = 30.0,
        max_seen_ids: int = 5000,
        initial_backfill_count: int = 15,
        auth_token: Optional[str] = None,
    ) -> None:
        self.api_url = api_url
        self.poll_interval = poll_interval
        self.max_seen_ids = max_seen_ids
        self.initial_backfill_count = initial_backfill_count
        import os
        self.auth_token = auth_token or os.getenv("GITHUB_TOKEN")
        self._seen_ids: Set[str] = set()
        self._seen_queue: Deque[str] = deque()
        self._running: bool = False

    def _mark_seen(self, item_id: str) -> None:
        if item_id in self._seen_ids:
            return
        self._seen_ids.add(item_id)
        self._seen_queue.append(item_id)
        if len(self._seen_queue) > self.max_seen_ids:
            old_id = self._seen_queue.popleft()
            self._seen_ids.discard(old_id)

    def normalize_raw_event(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalizes a raw GitHub event dict into the PulseStream event format."""
        if not raw_data or not isinstance(raw_data, dict):
            return None

        event_id_raw = str(raw_data.get("id") or "")
        gh_type = raw_data.get("type", "PushEvent")
        mapped_type = GH_EVENT_TYPE_MAP.get(gh_type, EventType.POST_CREATED)

        actor = raw_data.get("actor", {})
        user_id = actor.get("login") or "github_user"

        repo = raw_data.get("repo", {})
        community_id = repo.get("name") or "unknown/repo"

        raw_payload = raw_data.get("payload", {})
        title = _format_gh_title(gh_type, community_id, raw_payload)

        # Parse GitHub timestamp: ISO 8601 string, e.g. "2026-09-27T18:00:00Z"
        created_at_raw = raw_data.get("created_at")
        if created_at_raw:
            try:
                dt = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                timestamp_ms = int(dt.timestamp() * 1000)
            except Exception:
                timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        else:
            timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        payload = {
            "title": title,
            "repo": community_id,
            "actor": user_id,
            "gh_type": gh_type,
            "gh_id": event_id_raw,
            "action": raw_payload.get("action", ""),
            "url": f"https://github.com/{community_id}",
        }

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": mapped_type.value,
            "source": EventSource.GITHUB.value,
            "timestamp": timestamp_ms,
            "user_id": str(user_id),
            "community_id": str(community_id),
            "payload": json.dumps(payload),
            "ingested_at": now_ms,
            "schema_version": "1.0",
        }

    async def _fetch_events(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        """Fetches latest events list from GitHub API."""
        async with session.get(self.api_url, timeout=aiohttp.ClientTimeout(total=10.0)) as resp:
            if resp.status == 403 or resp.status == 429:
                remaining = resp.headers.get("X-RateLimit-Remaining", "0")
                reset_time = resp.headers.get("X-RateLimit-Reset")
                logger.warning(
                    "GitHub API rate limit hit (remaining=%s, reset=%s). Pausing.",
                    remaining,
                    reset_time,
                )
                raise aiohttp.ClientResponseError(
                    resp.request_info,
                    resp.history,
                    status=resp.status,
                    message="Rate limit reached",
                )
            if resp.status != 200:
                raise aiohttp.ClientError(f"GitHub API returned HTTP {resp.status}")

            data = await resp.json()
            if isinstance(data, list):
                return data
            return []

    async def stream_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Polls GitHub Events API for new activity, yielding normalized events."""
        self._running = True
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/vnd.github.v3+json",
        }
        if self.auth_token:
            headers["Authorization"] = f"token {self.auth_token}"
        attempt = 0
        first_poll = True

        logger.info("Starting GitHub Events connector against %s...", self.api_url)

        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10.0, sock_read=30.0)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            while self._running:
                try:
                    events_data = await self._fetch_events(session)
                    attempt = 0

                    # Filter new events by ID
                    new_events: List[Dict[str, Any]] = []
                    for item in events_data:
                        iid = str(item.get("id") or "")
                        if not iid:
                            continue
                        if first_poll:
                            if len(new_events) < self.initial_backfill_count:
                                new_events.append(item)
                            self._mark_seen(iid)
                        else:
                            if iid not in self._seen_ids:
                                self._mark_seen(iid)
                                new_events.append(item)

                    if first_poll:
                        first_poll = False

                    for item in new_events:
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
                    delay = calculate_backoff_delay(attempt, base_ms=3000.0, max_ms=60000.0)
                    logger.warning(
                        "GitHub Events API issue: %s. Retrying in %.2fs (attempt %d)...",
                        exc,
                        delay,
                        attempt,
                    )
                    await asyncio.sleep(delay)

                except Exception as e:
                    if not self._running:
                        break
                    logger.exception("Unexpected error in GitHub Events connector: %s", e)
                    await asyncio.sleep(5.0)

        logger.info("GitHubEventsConnector stream ended.")

    def stop(self) -> None:
        """Stops the polling loop."""
        self._running = False
