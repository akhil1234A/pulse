"""Unit tests for database module."""

from unittest.mock import AsyncMock, MagicMock
import pytest
from src.common.db import EVENT_COLUMNS, batch_insert_events, insert_dead_letter_event
from src.common.models import EventSource, EventType, PulseStreamEvent


def test_event_columns_schema():
    assert "event_id" in EVENT_COLUMNS
    assert "event_type" in EVENT_COLUMNS
    assert "timestamp" in EVENT_COLUMNS
    assert "payload" in EVENT_COLUMNS


@pytest.mark.asyncio
async def test_batch_insert_events_empty():
    mock_pool = MagicMock()
    inserted = await batch_insert_events(mock_pool, [])
    assert inserted == 0


@pytest.mark.asyncio
async def test_batch_insert_events_mock():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()

    # Asyncpg conn.transaction() returns an async context manager
    mock_tx = MagicMock()
    mock_tx.__aenter__ = AsyncMock(return_value=None)
    mock_tx.__aexit__ = AsyncMock(return_value=None)
    mock_conn.transaction = MagicMock(return_value=mock_tx)

    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    events = [
        PulseStreamEvent(
            event_type=EventType.POST_CREATED,
            source=EventSource.WIKIPEDIA,
            user_id="user_db_1",
            community_id="comm_1",
            payload={"action": "edit"},
        ),
        PulseStreamEvent(
            event_type=EventType.COMMENT_CREATED,
            source=EventSource.SIMULATOR,
            user_id="user_db_2",
            community_id="comm_2",
            payload={"action": "comment"},
        ),
    ]

    count = await batch_insert_events(mock_pool, events)

    assert count == 2
    mock_conn.copy_records_to_table.assert_called_once()
    args, kwargs = mock_conn.copy_records_to_table.call_args
    assert args[0] == "events"
    assert kwargs["columns"] == EVENT_COLUMNS
    assert len(kwargs["records"]) == 2


@pytest.mark.asyncio
async def test_insert_dead_letter_event():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
    mock_conn.fetchrow.return_value = {"id": 101}

    dlq_id = await insert_dead_letter_event(
        mock_pool,
        raw_event={"bad": "data"},
        error_message="Avro schema mismatch",
        error_type="VALIDATION_ERROR",
        retry_count=1,
    )

    assert dlq_id == 101
    mock_conn.fetchrow.assert_called_once()
