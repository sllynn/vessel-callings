# Databricks notebook source
# MAGIC %md
# MAGIC # 10 / load_shapes
# MAGIC
# MAGIC Reads the three shape parquet files from the landing volume, decodes
# MAGIC WKB → `GEOMETRY(4326)`, and writes a unified `shapes_raw` Delta table
# MAGIC with columns `(shape_id, source, category, name, geom, source_attrs,
# MAGIC valid_from, valid_to)`. Idempotent: REPLACE on every run.

# COMMAND ----------

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "clarksons")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

shapes_dir = f"/Volumes/{catalog}/{schema}/{volume}/shapes"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage the three sources as temp views

# COMMAND ----------

spark.read.parquet(
    f"{shapes_dir}/marine_regions_global_oceans_seas_v01_simple.parquet"
).createOrReplaceTempView("ocean_raw")

spark.read.parquet(
    f"{shapes_dir}/marine_regions_global_eezs_simple.parquet"
).createOrReplaceTempView("eez_raw")

spark.read.parquet(
    f"{shapes_dir}/UKHO_IMO_Routeing_Measures_Areas.parquet"
).createOrReplaceTempView("imo_raw")

for v in ("ocean_raw", "eez_raw", "imo_raw"):
    n = spark.table(v).count()
    print(f"  {v:12s}  {n:>4d} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build `shapes_raw` — unified columns, WKB → GEOMETRY(4326)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.shapes_raw AS
SELECT
  row_number() OVER (ORDER BY source, name) AS shape_id,
  source,
  category,
  name,
  geom,
  source_attrs,
  current_timestamp() AS valid_from,
  CAST(NULL AS TIMESTAMP)  AS valid_to
FROM (
  -- Ocean basins (10 features)
  SELECT
    'marine_regions_oceans' AS source,
    'ocean_basin'           AS category,
    name                    AS name,
    ST_GeomFromWKB(geometry, 4326) AS geom,
    map(
      'area_km2', CAST(area_km2 AS STRING)
    ) AS source_attrs
  FROM ocean_raw

  UNION ALL

  -- EEZs (285 features)
  SELECT
    'marine_regions_eez' AS source,
    'eez'                AS category,
    GEONAME              AS name,
    ST_GeomFromWKB(geometry, 4326) AS geom,
    map(
      'pol_type',   POL_TYPE,
      'territory1', TERRITORY1,
      'sovereign1', SOVEREIGN1,
      'iso_sov1',   ISO_SOV1,
      'area_km2',   CAST(AREA_KM2 AS STRING),
      'mrgid',      CAST(MRGID    AS STRING)
    ) AS source_attrs
  FROM eez_raw

  UNION ALL

  -- UKHO IMO routeing (157 features)
  SELECT
    'ukho_imo_routeing'             AS source,
    feature_ty                      AS category,
    coalesce(inform, feature_ty)    AS name,
    ST_GeomFromWKB(geometry, 4326)  AS geom,
    map(
      'restrn',        restrn,
      'globalid',      globalid,
      'st_area_sh_m2', CAST(st_area_sh AS STRING),
      'db_2dr46_h',    CAST(db_2dr46_h AS STRING)
    ) AS source_attrs
  FROM imo_raw
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — row counts per (source, category)

# COMMAND ----------

display(spark.sql(f"""
SELECT source, category, count(*) AS n
FROM {catalog}.{schema}.shapes_raw
GROUP BY source, category
ORDER BY source, n DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Antimeridian spot-check
# MAGIC
# MAGIC Any shape whose bounding box spans both ends of the longitude range
# MAGIC needs splitting before `h3_tessellateaswkb` (planar `GEOMETRY` treats
# MAGIC the antimeridian as a discontinuity).

# COMMAND ----------

antimeridian = spark.sql(f"""
SELECT shape_id, source, name,
       ST_XMin(geom) AS xmin,
       ST_XMax(geom) AS xmax
FROM {catalog}.{schema}.shapes_raw
WHERE ST_XMin(geom) < -179 AND ST_XMax(geom) > 179
""")

antimeridian_rows = antimeridian.collect()
if antimeridian_rows:
    print("WARNING: shapes spanning the antimeridian (need a split before tessellation):")
    antimeridian.show(truncate=False)
else:
    print("OK: no antimeridian-crossing shapes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish a summary back to the bundle CLI

# COMMAND ----------

import json

counts = spark.sql(f"""
SELECT
  source,
  count(*) AS n
FROM {catalog}.{schema}.shapes_raw
GROUP BY source
ORDER BY source
""").collect()

summary = {
  "total":                sum(r["n"] for r in counts),
  "by_source":            {r["source"]: r["n"] for r in counts},
  "antimeridian_count":   len(antimeridian_rows),
  "antimeridian_names":   [r["name"] for r in antimeridian_rows],
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
