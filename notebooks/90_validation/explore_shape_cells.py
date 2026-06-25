# Databricks notebook source
# MAGIC %md
# MAGIC # 90 / explore_shape_cells
# MAGIC
# MAGIC Visual sanity-check on the `shape_cells` tessellation: pick a handful of
# MAGIC UK-relevant shapes, render their original polygon outline plus the H3
# MAGIC cells covering them, and inspect via `geopandas.GeoDataFrame.explore()`.
# MAGIC
# MAGIC Looks for:
# MAGIC - cells form a continuous covering with no gaps or strays
# MAGIC - boundary cells (`core = FALSE`) cluster along the polygon edge
# MAGIC - core cells (`core = TRUE`) fill the interior
# MAGIC - shapes at different `target_res` show visibly different cell sizes
# MAGIC - the antimeridian + quarantine exclusions look right

# COMMAND ----------

# MAGIC %pip install uv

# COMMAND ----------

# MAGIC %sh uv pip install -r ../../requirements.lock

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import h3
import geopandas as gpd
import pandas as pd
from shapely import wkb
from shapely.geometry import Polygon

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "marineintel")
dbutils.widgets.text(
    "name_filter",
    "United Kingdom|Dover|Casquets|Scilly|Shetland|Orkney|North Atlantic|North Sea|Norwegian|Irish|French|German",
    "Pipe-separated regex for shape names to include",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
name_filter = dbutils.widgets.get("name_filter")

shapes_tbl = f"{catalog}.{schema}.shapes_raw"
cells_tbl  = f"{catalog}.{schema}.shape_cells"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pick the shapes — UK-relevant, tessellated (i.e. safe + not quarantined)

# COMMAND ----------

shapes_pdf = spark.sql(f"""
SELECT
  shape_id, source, category, name, target_res,
  ST_AsBinary(geom) AS geom_wkb
FROM {shapes_tbl}
WHERE tessellation_safe
  AND NOT coalesce(tessellation_quarantine, false)
  AND target_res IS NOT NULL
  AND name RLIKE '{name_filter}'
ORDER BY target_res, source, name
""").toPandas()

print(f"selected {len(shapes_pdf)} shapes")
shapes_pdf[["shape_id", "source", "category", "name", "target_res"]]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull the cells for those shapes

# COMMAND ----------

shape_ids = shapes_pdf["shape_id"].tolist()
cells_pdf = spark.sql(f"""
SELECT shape_id, cell, core, target_res
FROM {cells_tbl}
WHERE shape_id IN ({','.join(str(s) for s in shape_ids)})
""").toPandas()

print(f"selected {len(cells_pdf):,} cells across {cells_pdf['shape_id'].nunique()} shapes")
cells_pdf.groupby(["shape_id"]).agg(
    n_cells=("cell", "count"),
    n_core=("core", "sum"),
).reset_index().merge(shapes_pdf[["shape_id", "name", "target_res"]], on="shape_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build GeoDataFrames
# MAGIC
# MAGIC * `shapes_gdf` — the original polygon outlines, one row per shape.
# MAGIC * `cells_gdf`  — one row per H3 cell, geometry derived from
# MAGIC   `h3.cell_to_boundary` (GeoJSON order = lon, lat) and joined back
# MAGIC   with the shape's name for tooltip use.

# COMMAND ----------

shapes_pdf["geometry"] = shapes_pdf["geom_wkb"].apply(wkb.loads)
shapes_gdf = gpd.GeoDataFrame(
    shapes_pdf.drop(columns=["geom_wkb"]),
    geometry="geometry",
    crs="EPSG:4326",
)


def h3_cell_to_polygon(cell_id: int) -> Polygon:
    # h3-py v4's default top-level API is string-based — pass the hex form of
    # the cell ID. cell_to_boundary returns (lat, lng) pairs; flip to (lng, lat)
    # for shapely's (x, y) convention.
    coords = h3.cell_to_boundary(h3.int_to_str(cell_id))
    return Polygon([(lng, lat) for lat, lng in coords])


cells_pdf["geometry"] = cells_pdf["cell"].apply(h3_cell_to_polygon)
cells_gdf = gpd.GeoDataFrame(
    cells_pdf.merge(shapes_pdf[["shape_id", "name", "source", "category"]], on="shape_id"),
    geometry="geometry",
    crs="EPSG:4326",
)
cells_gdf["core_label"] = cells_gdf["core"].map({True: "core", False: "boundary"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Render — H3 cells coloured by core/boundary, with shape outlines on top

# COMMAND ----------

m = cells_gdf.explore(
    column="core_label",
    cmap={"core": "#3aa3ff", "boundary": "#ff8a3a"},
    style_kwds={"weight": 0.3, "fillOpacity": 0.45},
    tooltip=["name", "source", "category", "target_res", "core_label"],
    name="H3 cells",
    legend=True,
)

shapes_gdf.boundary.explore(
    m=m,
    color="#222",
    style_kwds={"weight": 1.6, "opacity": 0.9},
    name="Original polygon outlines",
)

import folium
folium.LayerControl(collapsed=False).add_to(m)

displayHTML(m._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-shape summary

# COMMAND ----------

display(
    spark.createDataFrame(
        cells_gdf.groupby(["shape_id", "name", "source", "category", "target_res"])
        .agg(n_cells=("cell", "count"), n_core=("core", "sum"))
        .assign(n_boundary=lambda df: df["n_cells"] - df["n_core"],
                core_pct=lambda df: (df["n_core"] / df["n_cells"] * 100).round(1))
        .reset_index()
        .sort_values("n_cells", ascending=False)
    )
)
