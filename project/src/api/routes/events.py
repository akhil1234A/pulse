"""Events query endpoints for PulseStream API."""

from datetime import datetime
import json
from typing import Any, Dict, List, Optional
import uuid
from fastapi import APIRouter, HTTPException, Query, Request, status

router = APIRouter(prefix="/api/v1/events", tags=["Events"])


def _format_event_row(row: Any) -> Dict[str, Any]:
    """Converts a database record into a JSON-serializable dictionary."""
    payload = row["payload"]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            pass

    return {
        "event_id": str(row["event_id"]),
        "event_type": row["event_type"],
        "source": row["source"],
        "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
        "user_id": row["user_id"],
        "community_id": row["community_id"],
        "payload": payload,
        "ingested_at": row["ingested_at"].isoformat() if row["ingested_at"] else None,
        "processed_at": row["processed_at"].isoformat() if row["processed_at"] else None,
        "schema_version": row["schema_version"],
    }


@router.get("")
async def list_events(
    request: Request,
    community_id: Optional[str] = Query(default=None, description="Filter by community ID"),
    event_type: Optional[str] = Query(default=None, description="Filter by event type"),
    source: Optional[str] = Query(default=None, description="Filter by event source"),
    start_time: Optional[datetime] = Query(default=None, description="Filter events >= start time"),
    end_time: Optional[datetime] = Query(default=None, description="Filter events <= end time"),
    page: int = Query(default=1, ge=1, description="Page number"),
    page_size: int = Query(default=20, ge=1, le=100, description="Items per page"),
) -> Dict[str, Any]:
    """Lists events with filtering and pagination from PostgreSQL."""
    db_pool = getattr(request.app.state, "db_pool", None)
    if db_pool is None:
        return {
            "items": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "total_pages": 0,
        }

    where_clauses: List[str] = []
    params: List[Any] = []
    idx = 1

    if community_id:
        where_clauses.append(f"community_id = ${idx}")
        params.append(community_id)
        idx += 1

    if event_type:
        where_clauses.append(f"event_type = ${idx}")
        params.append(event_type)
        idx += 1

    if source:
        where_clauses.append(f"source = ${idx}")
        params.append(source)
        idx += 1

    if start_time:
        where_clauses.append(f"timestamp >= ${idx}")
        params.append(start_time)
        idx += 1

    if end_time:
        where_clauses.append(f"timestamp <= ${idx}")
        params.append(end_time)
        idx += 1

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    offset = (page - 1) * page_size

    count_query = f"SELECT COUNT(*) FROM events {where_sql}"
    data_query = f"""
        SELECT event_id, event_type, source, timestamp, user_id, community_id,
               payload, ingested_at, processed_at, schema_version
        FROM events
        {where_sql}
        ORDER BY timestamp DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """

    async with db_pool.acquire() as conn:
        total_count = await conn.fetchval(count_query, *params)
        data_params = list(params) + [page_size, offset]
        rows = await conn.fetch(data_query, *data_params)

    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
    items = [_format_event_row(r) for r in rows]

    return {
        "items": items,
        "total": total_count,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.get("/{event_id}")
async def get_event_by_id(request: Request, event_id: str) -> Dict[str, Any]:
    """Retrieves a single event by UUID."""
    try:
        parsed_uuid = uuid.UUID(event_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Event ID '{event_id}' is not a valid UUID",
        )

    db_pool = getattr(request.app.state, "db_pool", None)
    if db_pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database pool unavailable",
        )

    query = """
        SELECT event_id, event_type, source, timestamp, user_id, community_id,
               payload, ingested_at, processed_at, schema_version
        FROM events
        WHERE event_id = $1
        LIMIT 1
    """

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(query, parsed_uuid)

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Event with ID '{event_id}' not found",
        )

    return _format_event_row(row)
