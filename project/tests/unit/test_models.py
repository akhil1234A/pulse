"""Unit tests for event data models."""

import json
import uuid
from datetime import datetime, timezone

from src.common.models import (
    EventSource,
    EventType,
    PulseStreamEvent,
)


def test_event_creation_defaults():
    event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.WIKIPEDIA,
        user_id="user_123",
        community_id="en.wikipedia",
        payload={"title": "Test Page", "comment": "Initial edit"},
    )

    assert uuid.UUID(event.event_id)
    assert event.event_type == EventType.POST_CREATED
    assert event.source == EventSource.WIKIPEDIA
    assert event.user_id == "user_123"
    assert event.community_id == "en.wikipedia"
    assert isinstance(event.payload, str)
    assert event.payload_dict["title"] == "Test Page"
    assert isinstance(event.timestamp, int)
    assert isinstance(event.ingested_at, int)
    assert event.schema_version == "1.0"


def test_event_timestamp_formats():
    now = datetime(2026, 9, 15, 12, 0, 0, tzinfo=timezone.utc)
    expected_millis = int(now.timestamp() * 1000)

    # Created with datetime
    event_dt = PulseStreamEvent(
        event_type=EventType.COMMENT_CREATED,
        source=EventSource.SIMULATOR,
        user_id="user_abc",
        timestamp=now,
    )
    assert event_dt.timestamp == expected_millis
    assert event_dt.timestamp_datetime == now

    # Created with ISO string
    event_iso = PulseStreamEvent(
        event_type=EventType.REACTION_ADDED,
        source=EventSource.SIMULATOR,
        user_id="user_abc",
        timestamp="2026-09-15T12:00:00Z",
    )
    assert event_iso.timestamp == expected_millis


def test_event_to_and_from_avro_dict():
    original = PulseStreamEvent(
        event_type=EventType.USER_JOINED,
        source=EventSource.SIMULATOR,
        user_id="user_999",
        community_id=None,
        payload=json.dumps({"badge": "newbie"}),
    )

    avro_dict = original.to_avro_dict()
    assert avro_dict["event_id"] == original.event_id
    assert avro_dict["event_type"] == "USER_JOINED"
    assert avro_dict["source"] == "SIMULATOR"
    assert avro_dict["community_id"] is None
    assert avro_dict["payload"] == json.dumps({"badge": "newbie"})

    reconstructed = PulseStreamEvent.from_avro_dict(avro_dict)
    assert reconstructed.event_id == original.event_id
    assert reconstructed.event_type == original.event_type
    assert reconstructed.source == original.source
    assert reconstructed.user_id == original.user_id
    assert reconstructed.community_id is None
    assert reconstructed.payload == original.payload


def test_to_db_record():
    event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.WIKIPEDIA,
        user_id="user_1",
        community_id="comm_1",
        payload={"action": "edit"},
    )

    record = event.to_db_record()
    assert len(record) == 10
    assert record[0] == uuid.UUID(event.event_id)
    assert record[1] == "POST_CREATED"
    assert record[2] == "WIKIPEDIA"
    assert isinstance(record[3], datetime)
    assert record[4] == "user_1"
    assert record[5] == "comm_1"
    assert record[6] == '{"action": "edit"}'
    assert isinstance(record[7], datetime)
    assert isinstance(record[8], datetime)
    assert record[9] == "1.0"
