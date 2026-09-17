"""Unit tests for the synthetic EventSimulator."""

import json
from collections import Counter

import pytest

from src.common.models import EventSource, EventType
from src.ingestion.connectors.simulator import COMMUNITIES, EventSimulator


def test_simulator_steady_single_event():
    sim = EventSimulator(mode="steady", rate=100)
    event = sim.generate_single_event()

    assert event["source"] == EventSource.SIMULATOR.value
    assert event["event_type"] in [e.value for e in EventType]
    assert event["user_id"].startswith("usr_")
    if event["event_type"] == EventType.USER_JOINED.value:
        assert event["community_id"] is None
    else:
        assert event["community_id"] in COMMUNITIES

    # Payload is valid JSON
    payload = json.loads(event["payload"])
    assert "score" in payload


def test_simulator_zipf_distribution_skew():
    sim = EventSimulator(mode="steady", rate=1000, num_users=50, zipf_alpha=1.5)
    counts = Counter()

    for _ in range(500):
        evt = sim.generate_single_event()
        counts[evt["user_id"]] += 1

    most_common = counts.most_common(5)
    least_common = counts.most_common()[-5:]

    # Top users should have significantly more events than bottom users due to Zipf skew
    top_avg = sum(c for _, c in most_common) / len(most_common)
    bottom_avg = sum(c for _, c in least_common) / len(least_common)
    assert top_avg > bottom_avg * 2


def test_simulator_chaos_duplicate_injection():
    sim = EventSimulator(mode="chaos", rate=100, chaos_duplicate_pct=1.0)
    # Prime recent events
    sim.generate_single_event()
    # Next event with 100% duplicate chance must be a duplicate
    dupe_event = sim.generate_single_event()
    assert dupe_event in sim._recent_events


def test_simulator_chaos_malformed_injection():
    sim = EventSimulator(mode="chaos", rate=100, chaos_duplicate_pct=0.0, chaos_malformed_pct=1.0)
    malformed = sim.generate_single_event()

    # Must be missing required fields or have invalid types
    is_malformed = (
        "event_type" not in malformed
        or malformed.get("event_type") == "UNKNOWN_ACTION_XYZ"
        or not isinstance(malformed.get("timestamp"), int)
    )
    assert is_malformed is True


@pytest.mark.asyncio
async def test_simulator_streaming_async_generator():
    sim = EventSimulator(mode="steady", rate=500)
    received = []

    async for event in sim.stream_events():
        received.append(event)
        if len(received) >= 10:
            sim.stop()
            break

    assert len(received) == 10
