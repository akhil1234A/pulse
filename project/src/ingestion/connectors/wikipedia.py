"""Wikipedia Recent Changes SSE stream connector with auto-reconnection and normalization."""

import asyncio
from datetime import datetime, timezone
import json
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional
import uuid

import aiohttp

from src.common.models import EventSource, EventType
from src.common.retry import calculate_backoff_delay

logger = logging.getLogger(__name__)

DEFAULT_WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "PulseStream/1.0 (https://github.com/pulsestream; dev@pulsestream.local) Python-aiohttp"

# Supported event type mapping per Technical Specification Section 4.1
WIKIPEDIA_EVENT_TYPE_MAP = {
    "edit": EventType.POST_CREATED,
    "new": EventType.POST_CREATED,
    "log": EventType.USER_JOINED,
    "categorize": EventType.REACTION_ADDED,
}

# Namespaces: 0 = Main article, 2 = User namespace
DEFAULT_ALLOWED_NAMESPACES = {0, 2}


class WikipediaConnector:
    """Consumes real-time recent changes from the Wikimedia EventStreams SSE endpoint.

    Features:
    - Namespace filtering (main article and user pages).
    - Mapping Wikipedia actions to PulseStream canonical event types.
    - Automatic reconnection with exponential backoff and jitter.
    - Compliant User-Agent headers.
    """

    def __init__(
        self,
        stream_url: str = DEFAULT_WIKIMEDIA_STREAM_URL,
        allowed_namespaces: Optional[set] = None,
        max_reconnect_attempts: int = 10,
    ) -> None:
        self.stream_url = stream_url
        self.allowed_namespaces = allowed_namespaces or DEFAULT_ALLOWED_NAMESPACES
        self.max_reconnect_attempts = max_reconnect_attempts
        self._running: bool = False

    def normalize_raw_event(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Maps and normalizes a Wikipedia recentchange item to the PulseStream envelope."""
        # Check namespace filter
        ns = raw_data.get("namespace")
        if ns is not None and ns not in self.allowed_namespaces:
            return None

        # Check action type
        wiki_type = raw_data.get("type")
        mapped_type = WIKIPEDIA_EVENT_TYPE_MAP.get(wiki_type)
        if not mapped_type:
            return None

        user_id = raw_data.get("user") or "anonymous_editor"
        wiki = raw_data.get("wiki") or "enwiki"

        # Timestamp: Wikimedia provides unix timestamp in seconds
        ts_raw = raw_data.get("timestamp")
        if isinstance(ts_raw, (int, float)):
            timestamp_ms = int(ts_raw * 1000)
        else:
            timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        payload = {
            "title": raw_data.get("title", ""),
            "comment": raw_data.get("comment", ""),
            "bot": raw_data.get("bot", False),
            "minor": raw_data.get("minor", False),
            "server_url": raw_data.get("server_url", ""),
            "wiki_url": raw_data.get("meta", {}).get("uri", ""),
            "length": raw_data.get("length", {}),
            "revision": raw_data.get("revision", {}),
        }

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": mapped_type.value,
            "source": EventSource.WIKIPEDIA.value,
            "timestamp": timestamp_ms,
            "user_id": str(user_id),
            "community_id": str(wiki),
            "payload": json.dumps(payload),
            "ingested_at": now_ms,
            "schema_version": "1.0",
        }

    async def stream_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Connects to Wikimedia SSE stream, yielding normalized event dicts with auto-reconnect."""
        self._running = True
        attempt = 0
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/event-stream",
        }

        logger.info("Connecting to Wikimedia EventStreams at %s...", self.stream_url)

        while self._running:
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=15.0, sock_read=60.0)
                async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                    async with session.get(self.stream_url) as response:
                        if response.status != 200:
                            logger.warning(
                                "Wikimedia stream returned HTTP status %d, reconnecting...",
                                response.status,
                            )
                            raise aiohttp.ClientError(f"HTTP {response.status}")

                        attempt = 0  # Reset reconnect backoff on successful connection
                        logger.info("Connected to Wikimedia EventStreams successfully.")

                        # Stream lines from SSE response
                        async for line in response.content:
                            if not self._running:
                                break

                            decoded = line.decode("utf-8", errors="replace").strip()
                            if not decoded or not decoded.startswith("data:"):
                                continue

                            json_str = decoded[5:].strip()
                            try:
                                raw_item = json.loads(json_str)
                            except json.JSONDecodeError:
                                continue

                            normalized = self.normalize_raw_event(raw_item)
                            if normalized:
                                yield normalized

            except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as exc:
                if not self._running:
                    break

                attempt += 1
                delay = calculate_backoff_delay(attempt, base_ms=500.0, max_ms=30000.0)
                logger.warning(
                    "Wikimedia SSE connection lost: %s. Reconnecting in %.2fs (attempt %d)...",
                    exc,
                    delay,
                    attempt,
                )
                await asyncio.sleep(delay)

            except Exception as e:
                if not self._running:
                    break
                logger.exception("Unexpected error in Wikipedia connector: %s", e)
                await asyncio.sleep(2.0)

        logger.info("WikipediaConnector stream ended.")

    def stop(self) -> None:
        """Stops the SSE stream consumption."""
        self._running = False
