"""Anomalies query endpoints for PulseStream API."""

import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

router = APIRouter(prefix="/api/v1/anomalies", tags=["Anomalies"])


def _format_anomaly_row(row: Any) -> Dict[str, Any]:
    """Converts a database anomaly record into a JSON dictionary."""
    details = row["details"]
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            pass

    event_ids = [str(eid) for eid in (row["event_ids"] or [])]

    return {
        "id": row["id"],
        "anomaly_type": row["anomaly_type"],
        "severity": row["severity"],
        "description": row["description"],
        "details": details,
        "detected_at": row["detected_at"].isoformat() if row["detected_at"] else None,
        "event_ids": event_ids,
    }


@router.get("")
async def list_anomalies(
    request: Request,
    limit: int = Query(default=20, ge=1, le=100, description="Maximum anomalies to return"),
    anomaly_type: Optional[str] = Query(default=None, description="Filter by anomaly type"),
    severity: Optional[str] = Query(
        default=None, description="Filter by severity (WARNING, CRITICAL)"
    ),
) -> List[Dict[str, Any]]:
    """Retrieves recent detected anomalies from PostgreSQL."""
    db_pool = getattr(request.app.state, "db_pool", None)
    if db_pool is None:
        return []

    where_clauses: List[str] = []
    params: List[Any] = []
    idx = 1

    if anomaly_type:
        where_clauses.append(f"anomaly_type = ${idx}")
        params.append(anomaly_type)
        idx += 1

    if severity:
        where_clauses.append(f"severity = ${idx}")
        params.append(severity)
        idx += 1

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT id, anomaly_type, severity, description, details, detected_at, event_ids
        FROM anomalies
        {where_sql}
        ORDER BY detected_at DESC
        LIMIT ${idx}
    """
    params.append(limit)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [_format_anomaly_row(r) for r in rows]
