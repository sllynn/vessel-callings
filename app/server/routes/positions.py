from fastapi import APIRouter, Depends, Query

from ..config import CATALOG, SCHEMA, user_access_token
from ..db import user_connection, rows_to_dicts

router = APIRouter(prefix="/api/positions", tags=["positions"])


@router.get("/latest")
def latest(token: str = Depends(user_access_token)):
    """One row per vessel — its most recent silver_positions record."""
    q = f"""
        SELECT vessel_id, vessel_name, vessel_type, mmsi,
               event_ts, lon, lat, sog, cog, nav_status
        FROM {CATALOG}.{SCHEMA}.silver_positions
        QUALIFY row_number() OVER (PARTITION BY vessel_id ORDER BY event_ts DESC) = 1
    """
    with user_connection(token) as conn, conn.cursor() as cur:
        cur.execute(q)
        return rows_to_dicts(cur)


@router.get("/track")
def track(
    vessel_id: str = Query(..., description="Vessel ID to fetch track for"),
    limit: int = Query(200, ge=1, le=2000),
    token: str = Depends(user_access_token),
):
    """Recent positions for a single vessel, ordered oldest → newest."""
    q = f"""
        SELECT event_ts, lon, lat, sog, cog
        FROM {CATALOG}.{SCHEMA}.silver_positions
        WHERE vessel_id = ?
        ORDER BY event_ts DESC
        LIMIT ?
    """
    with user_connection(token) as conn, conn.cursor() as cur:
        cur.execute(q, (vessel_id, limit))
        rows = rows_to_dicts(cur)
    return list(reversed(rows))
