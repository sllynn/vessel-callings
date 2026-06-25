# Databricks notebook source
# MAGIC %md
# MAGIC # 30 / stream_callings — streaming foreachBatch MERGE
# MAGIC
# MAGIC Continuous streaming pipeline that maintains `callings_gold`:
# MAGIC
# MAGIC 1. **Source** — `readStream` on `silver_positions` (Delta change stream).
# MAGIC 2. **Per micro-batch** — ancestor-expand through `h3_toparent` at each
# MAGIC    distinct `target_res`, equi-join `shape_cells`, chip-refine boundary
# MAGIC    cells, compute within-batch presence runs, and MERGE into
# MAGIC    `callings_gold` with cross-batch run continuation.
# MAGIC 3. **Trigger** — `processingTime` driven by `merge_trigger_seconds`.
# MAGIC
# MAGIC Cross-batch run continuation: for each (vessel_id, shape_id) appearing
# MAGIC in the batch, look up the currently-open calling. If the batch's
# MAGIC earliest match is within `gap_minutes` of that calling's `exit_ts`,
# MAGIC we treat the batch's matches as a continuation — entry_ts stays the
# MAGIC same, exit_ts and n_positions update. Otherwise we start a new calling.

# COMMAND ----------

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "marineintel")
dbutils.widgets.text("merge_trigger_seconds", "30")
dbutils.widgets.text("gap_minutes", "5")
dbutils.widgets.dropdown("bitemporal", "true", ["true", "false"])
dbutils.widgets.dropdown("reset_state", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
trigger_seconds = int(dbutils.widgets.get("merge_trigger_seconds"))
gap_minutes = float(dbutils.widgets.get("gap_minutes"))
gap_seconds = int(gap_minutes * 60)
bitemporal = dbutils.widgets.get("bitemporal") == "true"
reset_state = dbutils.widgets.get("reset_state") == "true"

silver       = f"{catalog}.{schema}.silver_positions"
shape_cells  = f"{catalog}.{schema}.shape_cells"
shapes_raw   = f"{catalog}.{schema}.shapes_raw"
matches_tbl  = f"{catalog}.{schema}.position_shape_matches"
callings_tbl = f"{catalog}.{schema}.callings_gold"
checkpoint   = f"/Volumes/{catalog}/{schema}/landing/_checkpoints/callings_stream"

print(f"silver       : {silver}")
print(f"shape_cells  : {shape_cells}")
print(f"matches      : {matches_tbl}")
print(f"callings     : {callings_tbl}")
print(f"trigger      : {trigger_seconds}s")
print(f"gap_seconds  : {gap_seconds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-compute the per-row ancestor-array literal
# MAGIC
# MAGIC Distinct `target_res` values across `shape_cells` are looked up once at
# MAGIC startup and inlined into the SQL — saves a sub-query in every batch.

# COMMAND ----------

target_res_values = [
    r["target_res"]
    for r in spark.sql(f"SELECT DISTINCT target_res FROM {shape_cells} ORDER BY target_res").collect()
]
assert target_res_values, "shape_cells contains no rows — run shape_index first"
print(f"target_res values present: {target_res_values}")

ancestor_array = "array(" + ", ".join(
    f"named_struct('ancestor_cell', h3_toparent(h3_cell, {r}), 'res', {r})"
    for r in target_res_values
) + ")"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure target tables exist with the right schema

# COMMAND ----------

if reset_state:
    print("reset_state=true — dropping matches + callings tables and clearing checkpoint")
    spark.sql(f"DROP TABLE IF EXISTS {matches_tbl}")
    spark.sql(f"DROP TABLE IF EXISTS {callings_tbl}")
    try:
        dbutils.fs.rm(checkpoint, recurse=True)
    except Exception as e:
        print(f"  rm({checkpoint}) → {e}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {matches_tbl} (
  vessel_id   STRING,
  vessel_name STRING,
  vessel_type STRING,
  mmsi        BIGINT,
  shape_id    BIGINT,
  event_ts    TIMESTAMP,
  lon         DOUBLE,
  lat         DOUBLE,
  sog         DOUBLE,
  cog         DOUBLE,
  nav_status  STRING,
  core        BOOLEAN,
  batch_id    BIGINT
)
CLUSTER BY (vessel_id, shape_id)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {callings_tbl} (
  vessel_id    STRING,
  shape_id     BIGINT,
  entry_ts     TIMESTAMP,
  exit_ts      TIMESTAMP,
  n_positions  BIGINT,
  valid_from   TIMESTAMP,
  valid_to     TIMESTAMP,
  as_of_ts     TIMESTAMP
)
CLUSTER BY (vessel_id, shape_id)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## foreachBatch — compute matches, runs, and MERGE
# MAGIC
# MAGIC Each micro-batch:
# MAGIC
# MAGIC 1. Stages incoming positions as `batch_positions`.
# MAGIC 2. Ancestor-expands + joins to `shape_cells` + chip-refines boundary
# MAGIC    cells → `batch_matches` (append to `position_shape_matches`).
# MAGIC 3. Computes within-batch runs via window-fn gap detection.
# MAGIC 4. Joins each batch-run against any currently-open calling for the
# MAGIC    same (vessel, shape) — if `batch_min - existing_exit <= gap`, the
# MAGIC    batch's run is an extension; entry_ts collapses to the existing
# MAGIC    value, n_positions accumulates.
# MAGIC 5. `MERGE INTO callings_gold` keyed on (vessel_id, shape_id, entry_ts).

# COMMAND ----------

def merge_micro_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Inside foreachBatch the batch DF carries its own SparkSession — using
    # the notebook's `spark` global hits a different session that can't see
    # `batch_positions`. Route all SQL through `s` instead.
    s = batch_df.sparkSession
    batch_df.createOrReplaceTempView("batch_positions")

    # 1. Compute matches for this batch + append to position_shape_matches.
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW batch_matches AS
    WITH position_ancestors AS (
      SELECT
        p.vessel_id, p.vessel_name, p.vessel_type, p.mmsi,
        p.event_ts, p.lon, p.lat, p.sog, p.cog, p.nav_status,
        a.ancestor_cell, a.res
      FROM batch_positions p
      LATERAL VIEW inline({ancestor_array}) a
    )
    SELECT
      pa.vessel_id, pa.vessel_name, pa.vessel_type, pa.mmsi,
      sc.shape_id, pa.event_ts,
      pa.lon, pa.lat, pa.sog, pa.cog, pa.nav_status,
      sc.core
    FROM position_ancestors pa
    JOIN {shape_cells} sc
      ON sc.cell       = pa.ancestor_cell
     AND sc.target_res = pa.res
    WHERE sc.core
       OR ST_Contains(ST_GeomFromWKB(sc.chip_wkb, 4326),
                      ST_Point(pa.lon, pa.lat, 4326))
    """)

    s.sql(f"""
    INSERT INTO {matches_tbl}
    SELECT vessel_id, vessel_name, vessel_type, mmsi, shape_id, event_ts,
           lon, lat, sog, cog, nav_status, core, {batch_id} AS batch_id
    FROM batch_matches
    """)

    # 2. Compute within-batch presence runs, then collapse with any active
    # calling for the same (vessel, shape) if the gap is within threshold.
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW batch_runs AS
    WITH ordered AS (
      SELECT vessel_id, shape_id, event_ts,
        lag(event_ts) OVER (PARTITION BY vessel_id, shape_id ORDER BY event_ts) AS prev_ts
      FROM batch_matches
    ),
    flagged AS (
      SELECT *,
        CASE
          WHEN prev_ts IS NULL THEN 1
          WHEN unix_timestamp(event_ts) - unix_timestamp(prev_ts) > {gap_seconds} THEN 1
          ELSE 0
        END AS is_new_run
      FROM ordered
    ),
    runs AS (
      SELECT *,
        sum(is_new_run) OVER (PARTITION BY vessel_id, shape_id ORDER BY event_ts
                              ROWS UNBOUNDED PRECEDING) AS run_id
      FROM flagged
    )
    SELECT vessel_id, shape_id,
           min(event_ts) AS batch_entry_ts,
           max(event_ts) AS batch_exit_ts,
           count(*)      AS batch_n,
           run_id
    FROM runs
    GROUP BY vessel_id, shape_id, run_id
    """)

    # 3. Collapse with active callings (cross-batch continuation). Only the
    # earliest run per (vessel, shape) in this batch can extend an existing
    # open calling — subsequent runs become new callings within this batch.
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW new_callings AS
    WITH ranked AS (
      SELECT *,
        row_number() OVER (PARTITION BY vessel_id, shape_id ORDER BY batch_entry_ts) AS rn
      FROM batch_runs
    ),
    active AS (
      -- The most-recent open calling per (vessel, shape). Without this
      -- row_number, a vessel that's been in/out/in of the same shape twice
      -- yields two source rows in the JOIN, and the MERGE blows up with
      -- DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE.
      SELECT vessel_id, shape_id, entry_ts AS existing_entry_ts,
             exit_ts AS existing_exit_ts, n_positions AS existing_n
      FROM (
        SELECT *,
          row_number() OVER (PARTITION BY vessel_id, shape_id
                             ORDER BY entry_ts DESC) AS rn
        FROM {callings_tbl}
        WHERE valid_to IS NULL
      )
      WHERE rn = 1
    )
    SELECT
      r.vessel_id, r.shape_id,
      CASE
        WHEN r.rn = 1 AND a.existing_entry_ts IS NOT NULL
          AND unix_timestamp(r.batch_entry_ts) - unix_timestamp(a.existing_exit_ts) <= {gap_seconds}
        THEN a.existing_entry_ts
        ELSE r.batch_entry_ts
      END AS entry_ts,
      r.batch_exit_ts AS exit_ts,
      CASE
        WHEN r.rn = 1 AND a.existing_entry_ts IS NOT NULL
          AND unix_timestamp(r.batch_entry_ts) - unix_timestamp(a.existing_exit_ts) <= {gap_seconds}
        THEN r.batch_n + a.existing_n
        ELSE r.batch_n
      END AS n_positions
    FROM ranked r
    LEFT JOIN active a USING (vessel_id, shape_id)
    """)

    s.sql(f"""
    MERGE INTO {callings_tbl} AS c
    USING new_callings AS n
    ON  c.vessel_id = n.vessel_id
    AND c.shape_id  = n.shape_id
    AND c.entry_ts  = n.entry_ts
    WHEN MATCHED THEN UPDATE SET
      c.exit_ts     = n.exit_ts,
      c.n_positions = n.n_positions,
      c.as_of_ts    = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
      vessel_id, shape_id, entry_ts, exit_ts, n_positions,
      valid_from, valid_to, as_of_ts
    ) VALUES (
      n.vessel_id, n.shape_id, n.entry_ts, n.exit_ts, n.n_positions,
      n.entry_ts, NULL, current_timestamp()
    )
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Start the streaming query

# COMMAND ----------

query = (spark.readStream
    .table(silver)
    # No `.withWatermark` — `foreachBatch` is a raw-batch sink and the
    # engine doesn't apply the watermark to anything we do inside. Late-
    # arrival handling happens IN foreachBatch via the `gap_seconds`
    # cross-batch continuation against the active callings_gold rows.
    .writeStream
    .queryName("callings_stream")
    .foreachBatch(merge_micro_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(processingTime=f"{trigger_seconds} seconds")
    .start()
)
print(f"started callings stream: query={query.id}")
query.awaitTermination()
