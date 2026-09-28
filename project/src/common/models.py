"""Data models for PulseStream events and domain types."""

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field, field_validator

from src.common.schemas.avro import deserialize_event, serialize_event


class EventType(str, Enum):
    POST_CREATED = "POST_CREATED"
    COMMENT_CREATED = "COMMENT_CREATED"
    REACTION_ADDED = "REACTION_ADDED"
    USER_JOINED = "USER_JOINED"


class EventSource(str, Enum):
    WIKIPEDIA = "WIKIPEDIA"
    SIMULATOR = "SIMULATOR"
    HACKERNEWS = "HACKERNEWS"
    GITHUB = "GITHUB"
    REDDIT = "REDDIT"
    DEVTO = "DEVTO"


def current_time_millis() -> int:
    """Returns current UTC timestamp in milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def datetime_to_millis(dt: datetime) -> int:
    """Converts a datetime object to milliseconds since epoch."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def millis_to_datetime(millis: int) -> datetime:
    """Converts milliseconds since epoch to a UTC datetime."""
    return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)


class PulseStreamEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    source: EventSource
    timestamp: Union[int, datetime] = Field(default_factory=current_time_millis)
    user_id: str
    community_id: Optional[str] = None
    payload: Union[str, Dict[str, Any]] = Field(default="{}")
    ingested_at: Union[int, datetime] = Field(default_factory=current_time_millis)
    schema_version: str = "1.0"

    @field_validator("payload", mode="before")
    @classmethod
    def normalize_payload(cls, v: Any) -> str:
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        if isinstance(v, str):
            return v
        return json.dumps(v)

    @field_validator("timestamp", "ingested_at", mode="before")
    @classmethod
    def normalize_timestamp(cls, v: Any) -> int:
        if isinstance(v, datetime):
            return datetime_to_millis(v)
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str):
            # Parse ISO string if provided
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return datetime_to_millis(dt)
        return current_time_millis()

    @property
    def timestamp_datetime(self) -> datetime:
        """Returns the event timestamp as a UTC datetime."""
        assert isinstance(self.timestamp, int)
        return millis_to_datetime(self.timestamp)

    @property
    def ingested_at_datetime(self) -> datetime:
        """Returns the ingested_at timestamp as a UTC datetime."""
        assert isinstance(self.ingested_at, int)
        return millis_to_datetime(self.ingested_at)

    @property
    def payload_dict(self) -> Dict[str, Any]:
        """Parses the payload string into a Python dict."""
        assert isinstance(self.payload, str)
        try:
            return json.loads(self.payload)
        except Exception:
            return {"raw": self.payload}

    def to_avro_dict(self) -> Dict[str, Any]:
        """Converts event to dictionary format required by the Avro schema."""
        return {
            "event_id": str(self.event_id),
            "event_type": self.event_type.value
            if isinstance(self.event_type, EventType)
            else str(self.event_type),
            "source": self.source.value
            if isinstance(self.source, EventSource)
            else str(self.source),
            "timestamp": int(self.timestamp),
            "user_id": str(self.user_id),
            "community_id": self.community_id,
            "payload": str(self.payload),
            "ingested_at": int(self.ingested_at),
            "schema_version": str(self.schema_version),
        }

    @classmethod
    def from_avro_dict(cls, data: Dict[str, Any]) -> "PulseStreamEvent":
        """Instantiates PulseStreamEvent from an Avro-deserialized dictionary."""
        return cls(
            event_id=data["event_id"],
            event_type=EventType(data["event_type"]),
            source=EventSource(data["source"]),
            timestamp=data["timestamp"],
            user_id=data["user_id"],
            community_id=data.get("community_id"),
            payload=data["payload"],
            ingested_at=data["ingested_at"],
            schema_version=data.get("schema_version", "1.0"),
        )

    def to_avro_bytes(self) -> bytes:
        """Serializes event directly to Avro binary bytes."""
        return serialize_event(self.to_avro_dict())

    @classmethod
    def from_avro_bytes(cls, data: bytes) -> "PulseStreamEvent":
        """Deserializes event from Avro binary bytes."""
        avro_dict = deserialize_event(data)
        return cls.from_avro_dict(avro_dict)

    def to_db_record(self) -> tuple:
        """Converts event to a record tuple matching the PostgreSQL events table schema.

        Columns: (event_id, event_type, source, timestamp, user_id,
        community_id, payload, ingested_at, processed_at, schema_version)
        """
        import uuid as _uuid

        event_uuid = _uuid.UUID(self.event_id) if isinstance(self.event_id, str) else self.event_id
        return (
            event_uuid,
            self.event_type.value,
            self.source.value,
            self.timestamp_datetime,
            self.user_id,
            self.community_id,
            self.payload,
            self.ingested_at_datetime,
            datetime.now(timezone.utc),
            self.schema_version,
        )
