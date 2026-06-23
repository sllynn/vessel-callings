from fastapi import APIRouter, Depends, Query

from ..config import CATALOG, SCHEMA, user_access_token
from ..db import user_connection, rows_to_dicts

router = APIRouter(prefix="/api/callings", tags=["callings"])


@router.get("/open")
def open_callings(token: str = Depends(user_access_token)):
    """All currently open callings (exit_ts IS NULL), joined to shape metadata."""
    q = f"""
        SELECT c.vessel_id, c.shape_id, c.entry_ts, c.last_seen_ts,
               c.n_positions, c.as_of_ts,
               s.name AS shape_name, s.source, s.category
        FROM {CATALOG}.{SCHEMA}.callings_gold c
        JOIN {CATALOG}.{SCHEMA}.shapes_raw s ON c.shape_id = s.shape_id
        WHERE c.exit_ts IS NULL
        ORDER BY c.entry_ts DESC
    """
    with user_connection(token) as conn, conn.cursor() as cur:
        cur.execute(q)
        return rows_to_dicts(cur)


@router.get("/history")
def history(
    vessel_id: str = Query(..., description="Vessel ID"),
    limit: int = Query(50, ge=1, le=500),
    token: str = Depends(user_access_token),
):
    """All callings (open and closed) for one vessel, newest entry first."""
    q = f"""
        SELECT c.shape_id, s.name AS shape_name, s.source, s.category,
               c.entry_ts, c.last_seen_ts, c.exit_ts, c.n_positions
        FROM {CATALOG}.{SCHEMA}.callings_gold c
        JOIN {CATALOG}.{SCHEMA}.shapes_raw s ON c.shape_id = s.shape_id
        WHERE c.vessel_id = ?
        ORDER BY c.entry_ts DESC
        LIMIT ?
    """
    with user_connection(token) as conn, conn.cursor() as cur:
        cur.execute(q, (vessel_id, limit))
        return rows_to_dicts(cur)
