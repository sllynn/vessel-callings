# Databricks notebook source
# MAGIC %md
# MAGIC # 90 / explore_vessel_callings
# MAGIC
# MAGIC Per-vessel forensic view. For a chosen `vessel_id`:
# MAGIC
# MAGIC * the full track from `silver_positions`,
# MAGIC * outlines of every shape the vessel ever called at,
# MAGIC * entry markers on the map (open vs closed),
# MAGIC * a tabular calling history.
# MAGIC
# MAGIC Default vessel `v-000028` — pick another via the widget.

# COMMAND ----------

# MAGIC %pip install uv

# COMMAND ----------

# MAGIC %sh uv pip install -r ../../requirements.lock

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import geopandas as gpd
import folium
import pandas as pd
from shapely import wkb
from shapely.geometry import LineString, Point

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "clarksons")
dbutils.widgets.text("vessel_id", "v-000028")
dbutils.widgets.text("simplify_deg", "0.02")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vessel_id = dbutils.widgets.get("vessel_id")
simplify_deg = float(dbutils.widgets.get("simplify_deg"))

silver       = f"{catalog}.{schema}.silver_positions"
callings_tbl = f"{catalog}.{schema}.callings_gold"
shapes_tbl   = f"{catalog}.{schema}.shapes_raw"

print(f"vessel_id    : {vessel_id}")
print(f"simplify_deg : {simplify_deg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull data: positions, callings, and shape outlines touched

# COMMAND ----------

positions_pdf = spark.sql(f"""
SELECT vessel_id, vessel_name, vessel_type, mmsi,
       event_ts, lon, lat, sog, cog, nav_status
FROM {silver}
WHERE vessel_id = '{vessel_id}'
ORDER BY event_ts
""").toPandas()

callings_pdf = spark.sql(f"""
SELECT c.vessel_id, c.shape_id, c.entry_ts, c.last_seen_ts, c.exit_ts,
       c.n_positions, c.as_of_ts,
       s.source, s.category, s.name AS shape_name
FROM {callings_tbl} c
JOIN {shapes_tbl} s ON c.shape_id = s.shape_id
WHERE c.vessel_id = '{vessel_id}'
ORDER BY c.entry_ts
""").toPandas()

print(f"positions : {len(positions_pdf):,}")
print(f"callings  : {len(callings_pdf):,}")
if positions_pdf.empty:
    raise RuntimeError(f"no positions found for vessel {vessel_id} in {silver}")

vessel_name = positions_pdf["vessel_name"].iloc[0]
vessel_type = positions_pdf["vessel_type"].iloc[0]
print(f"name      : {vessel_name}  type: {vessel_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shape outlines — the ones this vessel touched

# COMMAND ----------

if callings_pdf.empty:
    shapes_gdf = gpd.GeoDataFrame(columns=["shape_id", "name", "source", "category", "geometry"],
                                  geometry="geometry", crs="EPSG:4326")
else:
    shape_ids = sorted(callings_pdf["shape_id"].unique().tolist())
    shapes_pdf = spark.sql(f"""
    SELECT shape_id, source, category, name, ST_AsBinary(geom) AS geom_wkb
    FROM {shapes_tbl}
    WHERE shape_id IN ({','.join(str(s) for s in shape_ids)})
    """).toPandas()
    shapes_pdf["geometry"] = shapes_pdf["geom_wkb"].apply(wkb.loads)
    shapes_gdf = gpd.GeoDataFrame(
        shapes_pdf.drop(columns=["geom_wkb"]),
        geometry="geometry", crs="EPSG:4326",
    )
    shapes_gdf["geometry"] = shapes_gdf.geometry.simplify(simplify_deg)

print(f"shapes    : {len(shapes_gdf)} (deduplicated from {len(callings_pdf)} callings)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Track + entry markers

# COMMAND ----------

# Simplify the track too — at default 0.02° (~2 km), visually identical for
# a globe-spanning vessel but folium-friendly.
track_geom = LineString(list(zip(positions_pdf["lon"], positions_pdf["lat"])))
track_geom = track_geom.simplify(simplify_deg / 2)
track_gdf = gpd.GeoDataFrame(
    [{
        "vessel_id":   vessel_id,
        "vessel_name": vessel_name,
        "vessel_type": vessel_type,
        "n_positions": len(positions_pdf),
        "first_ts":    positions_pdf["event_ts"].iloc[0],
        "last_ts":     positions_pdf["event_ts"].iloc[-1],
        "geometry":    track_geom,
    }],
    geometry="geometry", crs="EPSG:4326",
)

# Per-calling event markers + connecting line. For each calling we plot
# up to three points (entry, last_seen, exit) derived by joining the
# stored timestamps back to silver_positions. The connecting LineString
# visually pairs them so each calling reads as a single unit on the map.
positions_idx = positions_pdf.set_index("event_ts")

def lookup_position(ts):
    """Return (lon, lat) at this event_ts, or (None, None) if not found."""
    if pd.isna(ts):
        return None, None
    try:
        p = positions_idx.loc[ts]
        if isinstance(p, pd.DataFrame):
            p = p.iloc[0]
        return float(p["lon"]), float(p["lat"])
    except KeyError:
        return None, None

entry_rows = []
last_seen_rows = []
exit_rows = []
link_rows = []

for cid, c in enumerate(callings_pdf.itertuples(index=False)):
    state = "open" if pd.isna(c.exit_ts) else "closed"
    base = {
        "calling_id":  cid,
        "shape_name":  c.shape_name,
        "source":      c.source,
        "category":    c.category,
        "entry_ts":    c.entry_ts,
        "last_seen":   c.last_seen_ts,
        "exit_ts":     c.exit_ts,
        "state":       state,
        "n_positions": int(c.n_positions),
    }

    e_lon, e_lat = lookup_position(c.entry_ts)
    l_lon, l_lat = lookup_position(c.last_seen_ts)
    x_lon, x_lat = lookup_position(c.exit_ts)

    if e_lon is not None:
        entry_rows.append({**base, "kind": "entry", "geometry": Point(e_lon, e_lat)})
    if l_lon is not None and (l_lon != e_lon or l_lat != e_lat):
        last_seen_rows.append({**base, "kind": "last_seen", "geometry": Point(l_lon, l_lat)})
    if x_lon is not None:
        exit_rows.append({**base, "kind": "exit", "geometry": Point(x_lon, x_lat)})

    # Connecting line: entry → last_seen → (exit if closed). Use whatever
    # points we have, with at least two needed for a LineString.
    line_coords = [pt for pt in [(e_lon, e_lat), (l_lon, l_lat), (x_lon, x_lat)] if pt[0] is not None]
    # Deduplicate adjacent identical points so LineString isn't degenerate
    dedup = [line_coords[0]] + [p for prev, p in zip(line_coords, line_coords[1:]) if p != prev]
    if len(dedup) >= 2:
        link_rows.append({**base, "geometry": LineString(dedup)})

entry_gdf     = gpd.GeoDataFrame(entry_rows,     geometry="geometry", crs="EPSG:4326") if entry_rows     else None
last_seen_gdf = gpd.GeoDataFrame(last_seen_rows, geometry="geometry", crs="EPSG:4326") if last_seen_rows else None
exit_gdf      = gpd.GeoDataFrame(exit_rows,      geometry="geometry", crs="EPSG:4326") if exit_rows      else None
link_gdf      = gpd.GeoDataFrame(link_rows,      geometry="geometry", crs="EPSG:4326") if link_rows      else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Render

# COMMAND ----------

# Layer 1: shape outlines (the ones the vessel called at) — coloured by source
source_colour = {
    "marine_regions_oceans": "#3aa3ff",
    "marine_regions_eez":    "#3cb44b",
    "ukho_imo_routeing":     "#e6194b",
}
present_sources = [s for s in source_colour if s in set(shapes_gdf["source"])] if not shapes_gdf.empty else []

if shapes_gdf.empty:
    m = folium.Map(location=[55, 0], zoom_start=4)
else:
    m = shapes_gdf.explore(
        column="source",
        categorical=True,
        categories=present_sources,
        cmap=[source_colour[s] for s in present_sources],
        style_kwds={"weight": 1.4, "fillOpacity": 0.12, "opacity": 0.8},
        tooltip=["name", "source", "category"],
        name="Shapes called at",
        legend=True,
    )

# Layer 2: track
track_gdf.explore(
    m=m,
    color="#222",
    style_kwds={"weight": 2.2, "opacity": 0.85},
    tooltip=["vessel_id", "vessel_name", "vessel_type", "n_positions", "first_ts", "last_ts"],
    name=f"Track of {vessel_id}",
)

# Layer 3a: connecting lines (entry → last_seen → exit) — drawn first
# so the markers sit on top.
if link_gdf is not None and not link_gdf.empty:
    link_gdf.explore(
        m=m,
        column="state",
        categorical=True,
        categories=["closed", "open"],
        cmap=["#888", "#0aa"],
        style_kwds={"weight": 2.5, "opacity": 0.65},
        tooltip=["shape_name", "category", "entry_ts", "last_seen", "exit_ts", "state"],
        name="Calling links (entry→last→exit)",
    )

# Layer 3b: entry markers — circles, GREEN
if entry_gdf is not None and not entry_gdf.empty:
    entry_gdf.explore(
        m=m,
        color="#1c8c1c",
        marker_kwds={"radius": 6},
        style_kwds={"fillOpacity": 0.95, "weight": 1.4, "color": "#0a4d0a"},
        tooltip=["shape_name", "category", "entry_ts", "last_seen", "exit_ts", "state", "n_positions"],
        name="Calling entries (●)",
    )

# Layer 3c: last_seen markers — small open circles, GREY (omitted when
# coincident with entry, since lookup_position dedupes)
if last_seen_gdf is not None and not last_seen_gdf.empty:
    last_seen_gdf.explore(
        m=m,
        color="#888",
        marker_kwds={"radius": 4},
        style_kwds={"fillOpacity": 0.4, "weight": 1.0, "color": "#444"},
        tooltip=["shape_name", "category", "entry_ts", "last_seen", "exit_ts", "state", "n_positions"],
        name="Last in-position (○)",
    )

# Layer 3d: exit markers — diamond marker via custom icon would be best,
# but folium.GeoJson rendering doesn't expose shapes cleanly through
# explore; using a RED filled circle as a clearly different colour is
# the next-best visual distinction.
if exit_gdf is not None and not exit_gdf.empty:
    exit_gdf.explore(
        m=m,
        color="#c1272d",
        marker_kwds={"radius": 6},
        style_kwds={"fillOpacity": 0.95, "weight": 1.4, "color": "#7a1a1f"},
        tooltip=["shape_name", "category", "entry_ts", "last_seen", "exit_ts", "state", "n_positions"],
        name="Calling exits (▲)",
    )

folium.LayerControl(collapsed=False).add_to(m)
displayHTML(m._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calling history (table)

# COMMAND ----------

if callings_pdf.empty:
    print("no callings yet for this vessel")
else:
    summary_pdf = callings_pdf[[
        "shape_name", "source", "category",
        "entry_ts", "last_seen_ts", "exit_ts", "n_positions", "as_of_ts",
    ]].copy()
    summary_pdf["state"] = summary_pdf["exit_ts"].apply(
        lambda v: "open" if pd.isna(v) else "closed"
    )
    summary_pdf["dwell_observed_min"] = (
        (summary_pdf["last_seen_ts"] - summary_pdf["entry_ts"]).dt.total_seconds() / 60.0
    ).round(1)
    display(spark.createDataFrame(summary_pdf))
