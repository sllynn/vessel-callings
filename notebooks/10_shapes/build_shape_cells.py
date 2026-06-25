# Databricks notebook source
# MAGIC %md
# MAGIC # 10 / build_shape_cells
# MAGIC
# MAGIC Tessellates `shapes_raw.geom` with `h3_tessellateaswkb` at each shape's
# MAGIC `target_res`, exploding to one row per cell. Writes `shape_cells` with
# MAGIC Liquid Clustering on `cell`.
# MAGIC
# MAGIC Skips shapes where `tessellation_safe = FALSE` (antimeridian crossers
# MAGIC await a proper split).

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "marineintel")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

shapes = f"{catalog}.{schema}.shapes_raw"
cells = f"{catalog}.{schema}.shape_cells"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build shape_cells
# MAGIC
# MAGIC `h3_tessellateaswkb` returns `ARRAY<STRUCT<cellid: BIGINT, core: BOOLEAN, chip: BINARY>>`.
# MAGIC Pass `ST_AsBinary(geom)` because the function accepts BINARY/GEOGRAPHY/STRING but
# MAGIC not GEOMETRY (see `feedback-geometry-over-geography` and `reference-h3-tessellate`).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {cells}
CLUSTER BY (cell)
AS
WITH safe_shapes AS (
  SELECT /*+ REPARTITION(64) */
    shape_id,
    target_res,
    simplify_tolerance,
    -- geom_simplified is the morphologically-opened-and-closed, lightly
    -- DP-simplified outline computed in pick_target_res — single source
    -- of truth shared with the app's /shapes/outlines endpoint. Fall
    -- back to raw geom if it hasn't been populated yet (e.g. running
    -- against an older shape_index materialisation).
    ST_AsBinary(coalesce(geom_simplified, geom)) AS geom_wkb
  FROM {shapes}
  WHERE tessellation_safe
    AND NOT coalesce(tessellation_quarantine, false)
    AND target_res IS NOT NULL
)
SELECT
  s.shape_id,
  s.target_res,
  t.cellid AS cell,
  t.core,
  -- Boundary-chip buffering, clipped back to the polygon.
  --
  -- h3_tessellateaswkb computes chips spheroidally and stores them as
  -- planar WKB; at large cell sizes the straight-line approximation of an
  -- H3 cell edge retreats inside the true spheroidal edge, leaving thin
  -- "no-chip" strips at every cell boundary that valid points can land in.
  -- Buffering the chip outward by `simplify_tolerance / 10` (~1.1 km for
  -- res-2 ocean basins, ~110 m for res-5 EEZs) bridges those strips.
  --
  -- But a plain outward buffer also pushes the chip OUT across the shape's
  -- own boundary, so points just outside the coastline / EEZ edge would
  -- falsely match. So we intersect the buffered chip back with the source
  -- polygon (`geom_wkb` — the same geometry we tessellated): the H3 hex
  -- edges, being interior to the polygon, keep their outward buffer and
  -- the seam stays closed, while the polygon-boundary edges of the chip
  -- snap back exactly to the true shape outline.
  CASE
    WHEN NOT t.core
     AND t.chip IS NOT NULL
     AND coalesce(s.simplify_tolerance, 0) > 0
    THEN ST_AsBinary(
           ST_Intersection(
             ST_Buffer(ST_GeomFromWKB(t.chip, 4326), s.simplify_tolerance / 10),
             ST_GeomFromWKB(s.geom_wkb, 4326)
           )
         )
    ELSE t.chip
  END AS chip_wkb
FROM safe_shapes s
LATERAL VIEW inline(h3_tessellateaswkb(s.geom_wkb, s.target_res)) t
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — core/boundary ratios and per-source cell counts

# COMMAND ----------

# Per-shape cell counts — feed both group-level max and a global top-N
per_shape = spark.sql(f"""
SELECT
  s.source, s.category, c.target_res, s.shape_id, s.name,
  count(*)                                     AS n_cells,
  sum(CASE WHEN c.core THEN 1 ELSE 0 END)      AS n_core,
  sum(CASE WHEN NOT c.core THEN 1 ELSE 0 END)  AS n_boundary
FROM {cells} c
JOIN {shapes} s USING (shape_id)
GROUP BY s.source, s.category, c.target_res, s.shape_id, s.name
""")
per_shape.createOrReplaceTempView("per_shape_cells")

rows = spark.sql("""
SELECT
  source, category, target_res,
  count(*)                AS n_shapes,
  sum(n_cells)            AS total_cells,
  sum(n_core)             AS total_core,
  sum(n_boundary)         AS total_boundary,
  max(n_cells)            AS max_cells_per_shape,
  round(avg(n_cells), 0)  AS avg_cells_per_shape
FROM per_shape_cells
GROUP BY source, category, target_res
ORDER BY target_res, source, category
""").collect()

heavy = spark.sql("""
SELECT name, source, target_res, n_cells, n_core, n_boundary
FROM per_shape_cells
ORDER BY n_cells DESC
LIMIT 10
""").collect()

total = spark.sql(f"SELECT count(*) FROM {cells}").collect()[0][0]

summary = {
    "total_cells": total,
    "by_source_category": [
        {
            "source": r["source"],
            "category": r["category"],
            "target_res": r["target_res"],
            "n_shapes": r["n_shapes"],
            "total_cells": r["total_cells"],
            "total_core": r["total_core"],
            "total_boundary": r["total_boundary"],
            "max_cells_per_shape": r["max_cells_per_shape"],
            "avg_cells_per_shape": r["avg_cells_per_shape"],
            "core_pct": round(100 * r["total_core"] / r["total_cells"], 1),
        }
        for r in rows
    ],
    "top_10_heavy_hitters": [
        {
            "name": r["name"],
            "source": r["source"],
            "target_res": r["target_res"],
            "n_cells": r["n_cells"],
            "n_core": r["n_core"],
            "n_boundary": r["n_boundary"],
        }
        for r in heavy
    ],
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
