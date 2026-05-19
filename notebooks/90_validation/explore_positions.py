# Databricks notebook source
# MAGIC %md
# MAGIC # 90 / explore_positions
# MAGIC
# MAGIC Visual sanity-check on `silver_positions` — builds a LineString per
# MAGIC vessel from its sequential AIS emissions and renders the lot on a folium
# MAGIC map via `geopandas.GeoDataFrame.explore`. Optional layers:
# MAGIC
# MAGIC - **vessel tracks**, coloured by `vessel_type`
# MAGIC - **NOAA shipping-lanes backdrop** (display-only file from §4.2)
# MAGIC - **shape_cells outlines** for a UK-relevant subset, as ground truth
# MAGIC
# MAGIC Looks for:
# MAGIC - tracks lie on plausible shipping corridors (Dover, Med, N. Atlantic)
# MAGIC - no vessels traversing land
# MAGIC - port arrivals visible as track endpoints near the named-port markers

# COMMAND ----------

# MAGIC %pip install uv

# COMMAND ----------

# MAGIC %sh uv pip install -r ../../requirements.lock

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json

import geopandas as gpd
import h3
import folium
from shapely import wkb
from shapely.geometry import LineString, Point, Polygon

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "clarksons")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("n_vessels", "30")  # cap to keep folium HTML in one cell
dbutils.widgets.text("bbox", "-15,45,15,65")  # xmin,ymin,xmax,ymax — UK & near neighbours
dbutils.widgets.text("simplify_deg", "0.02")  # geometry simplification tolerance in degrees
dbutils.widgets.text("shapes_filter",
                     "United Kingdom|Dover|Casquets|Scilly|Shetland|Orkney|North Atlantic|North Sea|Norwegian|Irish")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
volume  = dbutils.widgets.get("volume")
n_vessels = int(dbutils.widgets.get("n_vessels"))
xmin, ymin, xmax, ymax = (float(x) for x in dbutils.widgets.get("bbox").split(","))
simplify_deg = float(dbutils.widgets.get("simplify_deg"))
shapes_filter = dbutils.widgets.get("shapes_filter")
print(f"bbox        : lon [{xmin}, {xmax}]  lat [{ymin}, {ymax}]")
print(f"simplify_deg: {simplify_deg}")


def total_coords(gdf) -> int:
    """Quick proxy for HTML weight — sum of vertex counts across all geometries.

    Handles MultiX (via .geoms), Polygons (exterior + interiors), and
    LineString/Point (.coords). Polygons raise NotImplementedError if you
    naively ask for .coords on them.
    """
    def n(g):
        if g is None:
            return 0
        if hasattr(g, "geoms"):  # MultiPolygon, MultiLineString, GeometryCollection
            return sum(n(p) for p in g.geoms)
        if hasattr(g, "exterior") and g.exterior is not None:  # Polygon
            return len(g.exterior.coords) + sum(len(r.coords) for r in g.interiors)
        try:
            return len(g.coords)
        except (NotImplementedError, AttributeError):
            return 0
    return int(gdf.geometry.map(n).sum())

silver = f"{catalog}.{schema}.silver_positions"
shapes_tbl = f"{catalog}.{schema}.shapes_raw"
cells_tbl  = f"{catalog}.{schema}.shape_cells"
lanes_path = f"/Volumes/{catalog}/{schema}/{volume}/shapes/Shipping_Lanes_v1.geojson"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull positions — pick the N vessels with the most emissions

# COMMAND ----------

# Bbox filter so the map doesn't melt folium. Take the top-N vessels by record
# count *within the bbox*, then pull only their positions inside the bbox.
top_vessels = spark.sql(f"""
SELECT vessel_id
FROM (
  SELECT vessel_id, count(*) AS n
  FROM {silver}
  WHERE lon BETWEEN {xmin} AND {xmax}
    AND lat BETWEEN {ymin} AND {ymax}
  GROUP BY vessel_id
  ORDER BY n DESC
  LIMIT {n_vessels}
)
""").rdd.flatMap(lambda r: r).collect()

if not top_vessels:
    raise RuntimeError(f"no vessels found in bbox ({xmin}, {ymin}, {xmax}, {ymax})")

positions_pdf = spark.sql(f"""
SELECT vessel_id, vessel_name, vessel_type, mmsi, event_ts, lon, lat, sog, cog, nav_status
FROM {silver}
WHERE vessel_id IN ({','.join("'" + v + "'" for v in top_vessels)})
  AND lon BETWEEN {xmin} AND {xmax}
  AND lat BETWEEN {ymin} AND {ymax}
ORDER BY vessel_id, event_ts
""").toPandas()

print(f"selected {positions_pdf['vessel_id'].nunique()} vessels, "
      f"{len(positions_pdf):,} positions total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build per-vessel track LineStrings

# COMMAND ----------

def build_track(group):
    if len(group) < 2:
        return None
    return LineString(list(zip(group["lon"], group["lat"])))

tracks = (
    positions_pdf
    .groupby(["vessel_id", "vessel_name", "vessel_type", "mmsi"], as_index=False)
    .agg(
        n_positions=("event_ts", "count"),
        first_ts=("event_ts", "min"),
        last_ts=("event_ts", "max"),
        geometry=("event_ts", "first"),  # placeholder; overwritten below
    )
)
# Rebuild geometry properly per group
tracks["geometry"] = (
    positions_pdf
    .groupby("vessel_id")
    .apply(build_track)
    .reindex(tracks["vessel_id"].values)
    .values
)
tracks = tracks.dropna(subset=["geometry"])

tracks_gdf = gpd.GeoDataFrame(tracks, geometry="geometry", crs="EPSG:4326")
before = total_coords(tracks_gdf)
tracks_gdf["geometry"] = tracks_gdf.geometry.simplify(simplify_deg / 2)  # tracks are finer than shapes
print(f"built {len(tracks_gdf)} track LineStrings — vertices {before:,} -> {total_coords(tracks_gdf):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull a UK-relevant shape_cells outline for ground truth

# COMMAND ----------

shapes_pdf = spark.sql(f"""
SELECT shape_id, source, category, name, target_res,
       ST_AsBinary(geom) AS geom_wkb
FROM {shapes_tbl}
WHERE tessellation_safe
  AND NOT coalesce(tessellation_quarantine, false)
  AND name RLIKE '{shapes_filter}'
ORDER BY target_res, source, name
""").toPandas()

shapes_pdf["geometry"] = shapes_pdf["geom_wkb"].apply(wkb.loads)
shapes_gdf = gpd.GeoDataFrame(shapes_pdf.drop(columns=["geom_wkb"]),
                              geometry="geometry", crs="EPSG:4326")
# Clip to bbox first (drops globe-spanning ocean polygons), then simplify
# heavily — UK EEZs have 10s of thousands of vertices and would dominate
# the folium HTML otherwise.
shapes_gdf = shapes_gdf.clip((xmin, ymin, xmax, ymax))
before = total_coords(shapes_gdf)
shapes_gdf["geometry"] = shapes_gdf.geometry.simplify(simplify_deg)
print(f"shapes: {len(shapes_gdf)} polygons — vertices {before:,} -> {total_coords(shapes_gdf):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the NOAA shipping-lanes backdrop

# COMMAND ----------

lanes_gdf = gpd.read_file(lanes_path)
# Explode the MultiLineStrings into one row per linestring fragment so styling
# applies per-segment.
lanes_gdf = lanes_gdf.explode(index_parts=False).reset_index(drop=True)
# Bbox-clip to the visible area so we're not embedding global lanes in HTML.
lanes_gdf = lanes_gdf.cx[xmin:xmax, ymin:ymax].reset_index(drop=True)
before = total_coords(lanes_gdf)
lanes_gdf["geometry"] = lanes_gdf.geometry.simplify(simplify_deg)
print(f"lanes: {len(lanes_gdf)} segments — vertices {before:,} -> {total_coords(lanes_gdf):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Render the map
# MAGIC
# MAGIC Layer order, bottom → top:
# MAGIC   1. NOAA shipping lanes (faint grey backdrop)
# MAGIC   2. Shape outlines (UK-relevant set, dark grey)
# MAGIC   3. Vessel tracks coloured by vessel_type

# COMMAND ----------

# 1. Backdrop lanes — drawn first as the base layer
lane_colour = {"Major": "#5a87b8", "Middle": "#a0b8d0", "Minor": "#c8d2e0"}
lane_cats = [t for t in ("Major", "Middle", "Minor") if t in set(lanes_gdf["Type"])]
m = lanes_gdf.explore(
    column="Type",
    categorical=True,
    categories=lane_cats,
    cmap=[lane_colour[t] for t in lane_cats],
    style_kwds={"weight": 1.0, "opacity": 0.5},
    name="NOAA shipping lanes (backdrop)",
    legend=True,
)

# 2. Shape outlines — UK-relevant subset
shapes_gdf.boundary.explore(
    m=m,
    color="#333",
    style_kwds={"weight": 1.3, "opacity": 0.85},
    name="Shape outlines (UK relevant)",
)

# 3. Vessel tracks — coloured by vessel_type
type_colour = {
    "container": "#e6194b",
    "bulk":      "#3cb44b",
    "tanker":    "#4363d8",
    "ferry":     "#f58231",
    "fishing":   "#911eb4",
}
type_cats = [t for t in type_colour if t in set(tracks_gdf["vessel_type"])]
tracks_gdf.explore(
    m=m,
    column="vessel_type",
    categorical=True,
    categories=type_cats,
    cmap=[type_colour[t] for t in type_cats],
    style_kwds={"weight": 1.8, "opacity": 0.85},
    tooltip=["vessel_name", "vessel_type", "mmsi", "n_positions", "first_ts", "last_ts"],
    name="Vessel tracks",
    legend=True,
)

folium.LayerControl(collapsed=False).add_to(m)
displayHTML(m._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by vessel type

# COMMAND ----------

import pandas as pd
summary_df = (
    tracks_gdf
    .groupby("vessel_type")
    .agg(n_vessels=("vessel_id", "count"),
         total_positions=("n_positions", "sum"),
         avg_positions=("n_positions", "mean"))
    .reset_index()
)
display(spark.createDataFrame(summary_df))
