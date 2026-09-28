"""Unit tests for Avro schema and serialization utilities."""

from src.common.models import EventSource, EventType, PulseStreamEvent
from src.common.schemas.avro import (
    deserialize_event,
    get_avro_schema,
    serialize_event,
    validate_event_dict,
)


def test_get_avro_schema():
    schema = get_avro_schema()
    assert schema["type"] == "record"
    assert schema["name"].endswith("PulseStreamEvent")
    field_names = [f["name"] for f in schema["fields"]]
    assert "event_id" in field_names
    assert "event_type" in field_names
    assert "payload" in field_names


def test_avro_serialization_roundtrip():
    event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.WIKIPEDIA,
        user_id="wiki_editor",
        community_id="en",
        payload={"diff_size": 42},
    )

    avro_dict = event.to_avro_dict()
    assert validate_event_dict(avro_dict) is True

    serialized_bytes = serialize_event(avro_dict)
    assert isinstance(serialized_bytes, bytes)
    assert len(serialized_bytes) > 0

    deserialized = deserialize_event(serialized_bytes)
    assert deserialized["event_id"] == event.event_id
    assert deserialized["event_type"] == "POST_CREATED"
    assert deserialized["source"] == "WIKIPEDIA"
    assert deserialized["payload"] == '{"diff_size": 42}'


def test_model_avro_bytes_roundtrip():
    original = PulseStreamEvent(
        event_type=EventType.REACTION_ADDED,
        source=EventSource.SIMULATOR,
        user_id="user_fastavro",
        community_id=None,
        payload="{}",
    )

    data = original.to_avro_bytes()
    restored = PulseStreamEvent.from_avro_bytes(data)

    assert restored.event_id == original.event_id
    assert restored.event_type == original.event_type
    assert restored.source == original.source
    assert restored.user_id == original.user_id
    assert restored.community_id is None


def test_avro_hackernews_roundtrip():
    hn_event = PulseStreamEvent(
        event_type=EventType.POST_CREATED,
        source=EventSource.HACKERNEWS,
        user_id="dang",
        community_id="news.ycombinator.com",
        payload={"title": "Announcing PulseStream", "score": 250},
    )

    data = hn_event.to_avro_bytes()
    restored = PulseStreamEvent.from_avro_bytes(data)
    assert restored.source == EventSource.HACKERNEWS
    assert restored.user_id == "dang"
    assert restored.community_id == "news.ycombinator.com"


def test_invalid_avro_validation():
    # Missing required fields
    invalid_dict = {
        "event_id": "123",
        "event_type": "INVALID_TYPE",
    }
    assert validate_event_dict(invalid_dict) is False
