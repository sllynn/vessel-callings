# Databricks notebook source
# MAGIC %md
# MAGIC # 10 / pick_target_res
# MAGIC
# MAGIC Adds `target_res` and `tessellation_safe` columns to `shapes_raw`
# MAGIC based on `target_res_strategy`. Required before `build_shape_cells`.
# MAGIC
# MAGIC * `category` strategy — per-source-category lookup (recommended).
# MAGIC * `area` strategy — area-binned heuristic (TODO).

# COMMAND ----------

# MAGIC %pip install -q shapely

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "marineintel")
dbutils.widgets.dropdown("target_res_strategy", "category", ["category", "area"])
dbutils.widgets.text("quarantine_wkb_bytes", "500000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
strategy = dbutils.widgets.get("target_res_strategy")
quarantine_threshold = int(dbutils.widgets.get("quarantine_wkb_bytes"))

table = f"{catalog}.{schema}.shapes_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure target_res + tessellation_safe columns exist

# COMMAND ----------

existing_cols = {r["col_name"] for r in spark.sql(f"DESCRIBE TABLE {table}").collect()}
for col_name, col_type in [
    ("target_res", "INT"),
    ("tessellation_safe", "BOOLEAN"),
    ("tessellation_quarantine", "BOOLEAN"),
    ("wkb_bytes", "BIGINT"),
    ("simplify_tolerance", "DOUBLE"),
    ("morphology_radius", "DOUBLE"),
    ("geom_simplified", "GEOMETRY(4326)"),
]:
    if col_name in existing_cols:
        print(f"  column {col_name} already present — skipping")
    else:
        spark.sql(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
        print(f"  added column {col_name} {col_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assign target_res, simplification tolerances, morphology radius
# MAGIC
# MAGIC | Category | target_res | morphology_radius | simplify_tolerance |
# MAGIC | --- | --- | --- | --- |
# MAGIC | ocean_basin | 2 | 0.05° (~5.5 km) | 0.05° (~5.5 km) |
# MAGIC | eez | 5 | 0.015° (~1.6 km) | 0.005° (~550 m) |
# MAGIC | All UKHO IMO routeing categories | 7 / 8 | 0 | 0 |
# MAGIC
# MAGIC `morphology_radius` is the feature size below which features are
# MAGIC smoothed away (mangroves, river-delta fingers, small islands, narrow
# MAGIC peninsulas). The polygon is then put through an open-then-close
# MAGIC sequence — `buffer(-r).buffer(+r).buffer(+r).buffer(-r)` — and a
# MAGIC light `ST_Simplify` cleans up the vertex count introduced by the
# MAGIC buffers' rounded corners. `geom_simplified` is the resulting
# MAGIC geometry; downstream consumers (`build_shape_cells`, app outlines)
# MAGIC read it directly — one source of truth.

# COMMAND ----------

if strategy == "category":
    spark.sql(f"""
    MERGE INTO {table} AS s
    USING (
      SELECT * FROM VALUES
        -- (category, target_res, simplify_tolerance_degrees, morphology_radius_degrees)
        ('ocean_basin',                     2, 0.05,  0.05),
        ('eez',                             5, 0.005, 0.015),
        ('Areas to be Avoided',             7, 0.0,   0.0),
        ('Deep Water Route Part',           7, 0.0,   0.0),
        ('Inshore Traffic Zones',           7, 0.0,   0.0),
        ('Two-way Routes',                  7, 0.0,   0.0),
        ('Precautionary Areas',             8, 0.0,   0.0),
        ('Traffic Separation Scheme Lanes', 8, 0.0,   0.0),
        ('Traffic Separation Zones',        8, 0.0,   0.0)
      AS m(cat, res, tol, morph)
    ) AS m
    ON s.category = m.cat
    WHEN MATCHED THEN UPDATE SET
      target_res         = m.res,
      simplify_tolerance = m.tol,
      morphology_radius  = m.morph
    """)
elif strategy == "area":
    raise NotImplementedError("area-based strategy is a TODO")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mark antimeridian-crossing shapes as unsafe
# MAGIC
# MAGIC GEOMETRY(4326) is planar — a polygon spanning longitude 180° appears
# MAGIC as a world-spanning artefact, and `h3_tessellateaswkb` would either
# MAGIC error or produce billions of cells. Until we implement a proper
# MAGIC longitude-180 split, these shapes are excluded from `shape_cells`.
# MAGIC
# MAGIC Expected unsafe count: 12 (4 Pacific-spanning oceans + 8 Pacific-island EEZs).

# COMMAND ----------

spark.sql(f"""
UPDATE {table}
SET tessellation_safe = (ST_XMin(geom) >= -179 OR ST_XMax(geom) <= 179)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute post-simplify WKB byte size and auto-quarantine the outliers
# MAGIC
# MAGIC The build_shape_cells stage applies `ST_Simplify(geom, 0.01)` to EEZ
# MAGIC geometries; other sources go through unsimplified. We compute the
# MAGIC same here so the byte counts match what the tessellation actually sees,
# MAGIC and auto-quarantine any shape that still exceeds the threshold.

# COMMAND ----------

# Compute geom_simplified — the single canonical "simplified outline" used
# downstream by build_shape_cells and the app's /shapes/outlines endpoint.
#
# Morphological open-then-close at radius r, then a light DP simplify:
#   open(g)  = buffer(-r) → buffer(+r)   removes thin protrusions
#   close(g) = buffer(+r) → buffer(-r)   fills thin notches
# The two consecutive dilations collapse to buffer(+2r), so the chain
# becomes (-r, +2r, -r): three buffer ops per polygon, not four.
#
# Parallelism: the SQL-side REPARTITION(64) hint kept losing to AQE's
# coalesce-by-bytes logic (450 rows × few KB each looks "small" to AQE
# even though each row drives expensive ST_Buffer work). mapInPandas
# pushes execution to per-partition Python and AQE can't override it.
# Same planar buffer semantics either way — Databricks' ST_Buffer on
# GEOMETRY(4326) is planar, and shapely.buffer on lon/lat is planar.

from pyspark.sql.types import StructType, StructField, LongType, BinaryType
import pandas as pd
from typing import Iterator
from shapely import wkb as shp_wkb


def morphology_partition(pdfs: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in pdfs:
        out_ids: list[int] = []
        out_wkbs: list[bytes] = []
        for _, row in pdf.iterrows():
            raw = bytes(row["geom_wkb"])
            r   = float(row["morphology_radius"] or 0.0)
            tol = float(row["simplify_tolerance"] or 0.0)
            try:
                g = shp_wkb.loads(raw)
                if r > 0:
                    g = g.buffer(-r).buffer(2 * r).buffer(-r)
                if tol > 0:
                    g = g.simplify(tol, preserve_topology=True)
                out = shp_wkb.dumps(g, hex=False)
            except Exception:
                # Defensive: invalid geometry, antimeridian artefacts, etc.
                # Fall back to the raw geometry so the row isn't lost.
                out = raw
            out_ids.append(int(row["shape_id"]))
            out_wkbs.append(out)
        yield pd.DataFrame({"shape_id": out_ids, "geom_simplified_wkb": out_wkbs})


simp_schema = StructType([
    StructField("shape_id", LongType()),
    StructField("geom_simplified_wkb", BinaryType()),
])

shapes_in = (
    spark.table(table)
    .selectExpr(
        "shape_id",
        "ST_AsBinary(geom) AS geom_wkb",
        "morphology_radius",
        "simplify_tolerance",
    )
    .repartition(64, "shape_id")  # explicit, not a hint — AQE can't undo it
)

simplified = shapes_in.mapInPandas(morphology_partition, schema=simp_schema)

staging_tbl = f"{table}__simplified_staging"
simplified.write.format("delta").mode("overwrite").saveAsTable(staging_tbl)

spark.sql(f"""
MERGE INTO {table} AS s
USING {staging_tbl} AS m
  ON s.shape_id = m.shape_id
WHEN MATCHED THEN UPDATE SET
  s.geom_simplified = ST_GeomFromWKB(m.geom_simplified_wkb, 4326)
""")

spark.sql(f"DROP TABLE {staging_tbl}")

# wkb_bytes reflects what build_shape_cells will actually tessellate —
# always the geom_simplified column from here on. Quarantine acts on this
# post-simplify size so a shape with aggressive tolerance / morphology
# gets a fair chance even if its raw WKB is huge.
spark.sql(f"""
UPDATE {table}
SET wkb_bytes = octet_length(ST_AsBinary(coalesce(geom_simplified, geom)))
""")

spark.sql(f"""
UPDATE {table}
SET tessellation_quarantine = ({quarantine_threshold} > 0 AND wkb_bytes > {quarantine_threshold})
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Heaviest shapes by post-simplify WKB byte size

# COMMAND ----------

display(spark.sql(f"""
SELECT
  shape_id, source, category, name,
  target_res,
  tessellation_safe,
  tessellation_quarantine,
  wkb_bytes,
  round(wkb_bytes / 1024.0, 1) AS wkb_kib
FROM {table}
WHERE tessellation_safe
ORDER BY wkb_bytes DESC
LIMIT 25
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

rows = spark.sql(f"""
SELECT
  category,
  target_res,
  count(*) AS n,
  sum(CASE WHEN tessellation_safe THEN 1 ELSE 0 END) AS n_safe,
  sum(CASE WHEN NOT tessellation_safe THEN 1 ELSE 0 END) AS n_unsafe
FROM {table}
WHERE target_res IS NOT NULL
GROUP BY category, target_res
ORDER BY target_res, category
""").collect()

unassigned = spark.sql(f"SELECT count(*) FROM {table} WHERE target_res IS NULL").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predicted cell count — shape_area / h3_cell_area
# MAGIC
# MAGIC The quotient `shape_area_km2 / h3_cell_area_km2[target_res]` estimates
# MAGIC how many H3 cells each shape will tessellate into. Outliers warn us
# MAGIC about the heavy hitters before we kick off `h3_tessellateaswkb`.
# MAGIC
# MAGIC `area_km2` lives in `source_attrs` (Marine Regions sources carry it
# MAGIC directly; UKHO's `st_area_sh` is in projected m² and converted here).

# COMMAND ----------

# Average H3 cell area in km² per resolution (from the H3 spec)
H3_CELL_AREA_KM2 = {
    0: 4_357_449.0, 1:  609_788.4, 2:  86_801.78, 3:  12_393.43,
    4:     1_770.35, 5:      252.90, 6:      36.129, 7:       5.161,
    8:         0.7373, 9:        0.1053, 10:       0.01505, 11:      0.002150,
    12:        0.000307, 13:     0.0000439, 14:    0.00000627, 15:   0.000000895,
}
spark.createDataFrame(
    [(r, a) for r, a in H3_CELL_AREA_KM2.items()],
    "res INT, h3_cell_area_km2 DOUBLE",
).createOrReplaceTempView("h3_cell_area")

predicted = spark.sql(f"""
WITH shape_bboxes AS (
  SELECT
    shape_id, source, category, name, target_res,
    -- Bbox dimensions are cheap metadata-level ops, unlike ST_Centroid /
    -- ST_Area which materialise full geometry traversals and were OOMing
    -- the cluster across the most complex EEZs (Indonesia, Canada).
    -- Bbox area overestimates the actual polygon area, but for the
    -- "which shapes will produce the most cells" question, it's a useful
    -- conservative proxy.
    ST_XMin(geom) AS xmin, ST_XMax(geom) AS xmax,
    ST_YMin(geom) AS ymin, ST_YMax(geom) AS ymax,
    (ST_XMax(geom) - ST_XMin(geom)) * (ST_YMax(geom) - ST_YMin(geom))
      * 12392.0 * abs(cos(radians((ST_YMin(geom) + ST_YMax(geom)) / 2)))
      AS bbox_area_km2
  FROM {table}
  WHERE target_res IS NOT NULL AND tessellation_safe
)
SELECT
  s.shape_id, s.source, s.category, s.name, s.target_res,
  s.bbox_area_km2 AS shape_area_km2,
  c.h3_cell_area_km2,
  CAST(s.bbox_area_km2 / c.h3_cell_area_km2 AS BIGINT) AS approx_n_cells
FROM shape_bboxes s
JOIN h3_cell_area c ON c.res = s.target_res
""")
predicted.createOrReplaceTempView("predicted_cells")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 25 heavy hitters

# COMMAND ----------

display(spark.sql("""
SELECT shape_id, source, category, name, target_res,
       round(shape_area_km2, 1) AS shape_area_km2,
       approx_n_cells
FROM predicted_cells
ORDER BY approx_n_cells DESC
LIMIT 25
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate by (source, target_res)

# COMMAND ----------

display(spark.sql("""
SELECT
  source, target_res,
  count(*)              AS n_shapes,
  sum(approx_n_cells)   AS approx_total_cells,
  max(approx_n_cells)   AS approx_max_cells,
  round(avg(approx_n_cells), 0) AS approx_avg_cells
FROM predicted_cells
GROUP BY source, target_res
ORDER BY target_res, source
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

agg_rows = spark.sql("""
SELECT source, target_res,
       count(*) AS n_shapes,
       sum(approx_n_cells) AS approx_total_cells,
       max(approx_n_cells) AS approx_max_cells
FROM predicted_cells
GROUP BY source, target_res
ORDER BY target_res, source
""").collect()

top_rows = spark.sql("""
SELECT name, target_res, approx_n_cells
FROM predicted_cells
ORDER BY approx_n_cells DESC
LIMIT 10
""").collect()

quarantined = spark.sql(f"""
SELECT shape_id, source, category, name, target_res, wkb_bytes
FROM {table}
WHERE tessellation_quarantine
ORDER BY wkb_bytes DESC
""").collect()

heaviest = spark.sql(f"""
SELECT shape_id, source, category, name, wkb_bytes
FROM {table}
WHERE tessellation_safe
ORDER BY wkb_bytes DESC
LIMIT 10
""").collect()

summary = {
    "strategy": strategy,
    "quarantine_threshold_bytes": quarantine_threshold,
    "by_category": [
        {"category": r["category"], "target_res": r["target_res"], "n": r["n"], "n_safe": r["n_safe"], "n_unsafe": r["n_unsafe"]}
        for r in rows
    ],
    "unassigned_category_count": unassigned,
    "total_safe": sum(r["n_safe"] for r in rows),
    "total_unsafe": sum(r["n_unsafe"] for r in rows),
    "predicted_cells_by_source": [
        {"source": r["source"], "target_res": r["target_res"], "n_shapes": r["n_shapes"],
         "approx_total_cells": r["approx_total_cells"], "approx_max_cells": r["approx_max_cells"]}
        for r in agg_rows
    ],
    "predicted_grand_total_cells": sum(r["approx_total_cells"] for r in agg_rows),
    "top_10_heavy_hitters_by_cells": [
        {"name": r["name"], "target_res": r["target_res"], "approx_n_cells": r["approx_n_cells"]}
        for r in top_rows
    ],
    "top_10_heaviest_by_wkb_bytes": [
        {"name": r["name"], "source": r["source"], "wkb_bytes": r["wkb_bytes"]}
        for r in heaviest
    ],
    "quarantined_shapes": [
        {"shape_id": r["shape_id"], "name": r["name"], "source": r["source"], "wkb_bytes": r["wkb_bytes"]}
        for r in quarantined
    ],
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
