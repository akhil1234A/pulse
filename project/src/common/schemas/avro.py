"""Avro serialization, deserialization, and schema validation helpers using fastavro."""

import io
import json
from pathlib import Path
from typing import Any, Dict
import fastavro

_SCHEMA_PATH = Path(__file__).parent / "event.avsc"
_PARSED_SCHEMA = None


def get_avro_schema() -> Dict[str, Any]:
    """Loads and caches parsed Avro schema."""
    global _PARSED_SCHEMA
    if _PARSED_SCHEMA is None:
        with open(_SCHEMA_PATH, "r", encoding="utf-8") as f:
            raw_schema = json.load(f)
        _PARSED_SCHEMA = fastavro.parse_schema(raw_schema)
    return _PARSED_SCHEMA


def validate_event_dict(event_dict: Dict[str, Any]) -> bool:
    """Validates an event dictionary against the Avro schema.

    Returns True if valid, False otherwise.
    """
    schema = get_avro_schema()
    try:
        return bool(fastavro.validate(event_dict, schema, raise_errors=False))
    except Exception:
        return False


def serialize_event(event_dict: Dict[str, Any]) -> bytes:
    """Serializes an event dictionary to Avro binary format (schemaless)."""
    schema = get_avro_schema()
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, event_dict)
    return buffer.getvalue()


def deserialize_event(data: bytes) -> Dict[str, Any]:
    """Deserializes Avro binary data into a Python dictionary."""
    schema = get_avro_schema()
    buffer = io.BytesIO(data)
    return fastavro.schemaless_reader(buffer, schema)
