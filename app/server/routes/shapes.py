import json
from fastapi import APIRouter, Depends, Query

from ..config import CATALOG, SCHEMA, user_access_token
from ..db import user_connection

router = APIRouter(prefix="/api/shapes", tags=["shapes"])


@router.get("/outlines")
def outlines(
    source: str = Query(None, description="Filter by source (omit for all)"),
    token: str = Depends(user_access_token),
):
    """All shape outlines as a single GeoJSON FeatureCollection.

    Reads `geom_simplified` directly — the morphologically-opened-and-closed
    and lightly DP-simplified outline computed once in `pick_target_res`.
    The same column is what `build_shape_cells` tessellates against, so the
    map shows exactly what's matched.
    """
    where = "WHERE tessellation_safe AND NOT coalesce(tessellation_quarantine, false)"
    params = []
    if source:
        where += " AND source = ?"
        params.append(source)

    q = f"""
        SELECT shape_id, source, category, name,
               ST_AsGeoJSON(coalesce(geom_simplified, geom)) AS geom_json
        FROM {CATALOG}.{SCHEMA}.shapes_raw
        {where}
    """

    features = []
    with user_connection(token) as conn, conn.cursor() as cur:
        cur.execute(q, params)
        for shape_id, src, category, name, geom_json in cur.fetchall():
            if not geom_json:
                continue
            features.append({
                "type": "Feature",
                "id": shape_id,
                "properties": {
                    "shape_id": shape_id,
                    "source": src,
                    "category": category,
                    "name": name,
                },
                "geometry": json.loads(geom_json),
            })

    return {"type": "FeatureCollection", "features": features}
