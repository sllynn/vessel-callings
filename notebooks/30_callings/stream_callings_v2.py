# Databricks notebook source
# MAGIC %md
# MAGIC # 30 / stream_callings_v2 — gaps-and-islands MERGE
# MAGIC
# MAGIC The §5.1 design from BRIEF.md. Per micro-batch:
# MAGIC
# MAGIC 1. **candidate_pairs** — three-source UNION:
# MAGIC    (a) `(V, S)` the batch produced an IN-match for,
# MAGIC    (b) open callings for vessels with any new position in the batch,
# MAGIC    (c) closed callings whose `(entry_ts, exit_ts)` envelope contains a
# MAGIC        new batch position.
# MAGIC 2. **vessel_lookback** — earliest `entry_ts` of any affected calling
# MAGIC    per vessel, capped at `retraction_window` before `max(event_ts)`.
# MAGIC 3. **relevant_positions** — all of vessel V's `silver_positions` rows
# MAGIC    from the lookback to now. Includes any late OUT-position the batch
# MAGIC    just brought in.
# MAGIC 4. **Renumber + match + group by `(pos_num − match_num)`** — the
# MAGIC    gaps-and-islands derivation. Each distinct diff value is one
# MAGIC    calling.
# MAGIC 5. **exit_ts derivation** — by construction, the position at
# MAGIC    `pos_num = last_pos_num + 1` (if it exists) is necessarily OUT for
# MAGIC    this shape. `exit_ts` = that position's `event_ts`; NULL otherwise.
# MAGIC 6. **BEGIN ATOMIC** UPDATE + INSERT + DELETE.
# MAGIC
# MAGIC No `gap_seconds` heuristic anywhere — closure semantics are entirely
# MAGIC observational.

# COMMAND ----------

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "clarksons")
dbutils.widgets.text("merge_trigger_seconds", "30")
dbutils.widgets.text("retraction_window_days", "7")
dbutils.widgets.dropdown("reset_state", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
trigger_seconds = int(dbutils.widgets.get("merge_trigger_seconds"))
retraction_window_days = int(dbutils.widgets.get("retraction_window_days"))
reset_state = dbutils.widgets.get("reset_state") == "true"

silver       = f"{catalog}.{schema}.silver_positions"
shape_cells  = f"{catalog}.{schema}.shape_cells"
shapes_raw   = f"{catalog}.{schema}.shapes_raw"
matches_tbl  = f"{catalog}.{schema}.position_shape_matches"
callings_tbl = f"{catalog}.{schema}.callings_gold"
staging_tbl  = f"{catalog}.{schema}._v2_batch_staging"
checkpoint   = f"/Volumes/{catalog}/{schema}/landing/_checkpoints/callings_stream_v2"

print(f"silver                  : {silver}")
print(f"shape_cells             : {shape_cells}")
print(f"matches                 : {matches_tbl}")
print(f"callings                : {callings_tbl}")
print(f"trigger                 : {trigger_seconds}s")
print(f"retraction_window_days  : {retraction_window_days}")
print(f"reset_state             : {reset_state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-compute ancestor expansion literal

# COMMAND ----------

target_res_values = [
    r["target_res"]
    for r in spark.sql(f"SELECT DISTINCT target_res FROM {shape_cells} ORDER BY target_res").collect()
]
assert target_res_values, "shape_cells contains no rows — run shape_index first"
print(f"target_res values: {target_res_values}")

# Use h3_longlatash3 at each target_res rather than h3_toparent(h3_cell, r):
# `h3_toparent` walks the H3 index hierarchy, which is NOT perfectly
# geometrically nested due to aperture-7 rotation (~19.1° between child
# and parent). For points near res-N cell boundaries the index-parent
# can be a different cell from the one geometrically containing the
# point — and the boundary chip stored under the index-parent then
# doesn't contain the point. `h3_longlatash3(lon, lat, r)` always
# returns the geometric cell, so the chip refinement evaluates against
# the right chip.
ancestor_array = "array(" + ", ".join(
    f"named_struct('ancestor_cell', h3_longlatash3(lon, lat, {r}), 'res', {r})"
    for r in target_res_values
) + ")"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset (optional) + ensure target tables exist with the refined schema
# MAGIC
# MAGIC `callings_gold` here uses the §5.1 schema:
# MAGIC
# MAGIC * `last_seen_ts` — latest observed in-position; monotonic.
# MAGIC * `exit_ts` — NULL while open; set to the first out-position observed
# MAGIC   after `last_seen_ts`. No `valid_from`/`valid_to` columns.

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
  vessel_id     STRING,
  shape_id      BIGINT,
  entry_ts      TIMESTAMP,
  last_seen_ts  TIMESTAMP,
  exit_ts       TIMESTAMP,
  n_positions   BIGINT,
  as_of_ts      TIMESTAMP
)
CLUSTER BY (vessel_id, shape_id)
TBLPROPERTIES (
  -- Customer requires the MERGE + DELETE inside foreachBatch to be
  -- collectively atomic so their front end never sees an inconsistent
  -- snapshot. `delta.feature.catalogManaged` unlocks `BEGIN ATOMIC ... END`
  -- transactional writes for this UC-managed Delta table.
  'delta.feature.catalogManaged' = 'supported'
)
""")

# Belt-and-brace: pick up the feature on a pre-existing table too. Idempotent.
spark.sql(f"""
ALTER TABLE {callings_tbl}
SET TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## foreachBatch — the gaps-and-islands derivation

# COMMAND ----------

def merge_micro_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Same sparkSession-routing fix as the v1 notebook
    s = batch_df.sparkSession

    # Materialise the batch as a catalog-managed Delta table so the
    # downstream SQL temp views (and ultimately the BEGIN ATOMIC block) can
    # reference it. BEGIN ATOMIC rejects temp views created via the
    # DataFrame API (`batch_df.createOrReplaceTempView(...)`) — it only
    # accepts SQL-defined views or persisted tables.
    (batch_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(staging_tbl))
    s.sql(f"ALTER TABLE {staging_tbl} "
          f"SET TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')")
    s.sql(f"CREATE OR REPLACE TEMP VIEW batch_positions AS SELECT * FROM {staging_tbl}")

    # ---------------------------------------------------------------------
    # 1. batch_matches — the new IN-matches in this batch. Same shape as v1.
    # ---------------------------------------------------------------------
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

    # Append the new matches to the audit log
    s.sql(f"""
    INSERT INTO {matches_tbl}
    SELECT vessel_id, vessel_name, vessel_type, mmsi, shape_id, event_ts,
           lon, lat, sog, cog, nav_status, core, {batch_id} AS batch_id
    FROM batch_matches
    """)

    # ---------------------------------------------------------------------
    # 2. candidate_pairs — three-source UNION (BRIEF §5.1)
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW candidate_pairs AS
    SELECT DISTINCT vessel_id, shape_id FROM batch_matches
    UNION
    -- Open callings for vessels with new positions in this batch
    SELECT c.vessel_id, c.shape_id
    FROM {callings_tbl} c
    WHERE c.exit_ts IS NULL
      AND c.vessel_id IN (SELECT DISTINCT vessel_id FROM batch_positions)
    UNION
    -- Closed callings whose envelope contains a new batch position (late-OUT splits)
    SELECT c.vessel_id, c.shape_id
    FROM {callings_tbl} c
    JOIN batch_positions p
      ON p.vessel_id = c.vessel_id
     AND p.event_ts BETWEEN c.entry_ts AND c.exit_ts
    WHERE c.exit_ts IS NOT NULL
    """)

    # ---------------------------------------------------------------------
    # 3. vessel_lookback — capped at retraction_window
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW vessel_lookback AS
    WITH cap AS (
      SELECT max(event_ts) - INTERVAL {retraction_window_days} DAYS AS cap_ts
      FROM {silver}
    )
    SELECT cp.vessel_id,
      GREATEST(
        LEAST(
          coalesce(MIN(c.entry_ts),  TIMESTAMP'9999-01-01'),
          coalesce(MIN(bm.event_ts), TIMESTAMP'9999-01-01')
        ),
        (SELECT cap_ts FROM cap)
      ) AS lookback_ts
    FROM candidate_pairs cp
    LEFT JOIN {callings_tbl} c
      ON c.vessel_id = cp.vessel_id AND c.shape_id = cp.shape_id
    LEFT JOIN batch_matches bm
      ON bm.vessel_id = cp.vessel_id AND bm.shape_id = cp.shape_id
    GROUP BY cp.vessel_id
    """)

    # ---------------------------------------------------------------------
    # 4. numbered_positions — global per-vessel position numbering
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW numbered_positions AS
    SELECT p.*,
      row_number() OVER (PARTITION BY p.vessel_id ORDER BY p.event_ts) AS pos_num
    FROM {silver} p
    JOIN vessel_lookback l ON l.vessel_id = p.vessel_id
    WHERE p.event_ts >= l.lookback_ts
    """)

    # ---------------------------------------------------------------------
    # 5. matched — H3 ancestor + chip refinement, scoped to candidate_pairs
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW matched AS
    WITH expanded AS (
      SELECT np.vessel_id, np.event_ts, np.pos_num, np.lon, np.lat,
             a.ancestor_cell, a.res
      FROM numbered_positions np
      LATERAL VIEW inline({ancestor_array}) a
    )
    SELECT e.vessel_id, sc.shape_id, e.event_ts, e.pos_num
    FROM expanded e
    JOIN {shape_cells} sc
      ON sc.cell       = e.ancestor_cell
     AND sc.target_res = e.res
    JOIN candidate_pairs cp
      ON cp.vessel_id = e.vessel_id AND cp.shape_id = sc.shape_id
    WHERE sc.core
       OR ST_Contains(ST_GeomFromWKB(sc.chip_wkb, 4326),
                      ST_Point(e.lon, e.lat, 4326))
    """)

    # ---------------------------------------------------------------------
    # 6. numbered_matched — per-(vessel, shape) match numbering
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW numbered_matched AS
    SELECT *,
      row_number() OVER (PARTITION BY vessel_id, shape_id ORDER BY event_ts) AS match_num
    FROM matched
    """)

    # ---------------------------------------------------------------------
    # 7+8. callings_from_diff — gaps-and-islands aggregation + exit_ts lookup
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW callings_from_diff AS
    WITH derived AS (
      SELECT vessel_id, shape_id,
        MIN(event_ts) AS entry_ts,
        MAX(event_ts) AS last_seen_ts,
        MAX(pos_num)  AS last_pos_num,
        COUNT(*)      AS n_positions
      FROM numbered_matched
      GROUP BY vessel_id, shape_id, (pos_num - match_num)
    )
    -- exit_ts: by construction, the position at (vessel_id, pos_num = last_pos_num+1)
    -- is necessarily OUT for this shape. LEFT JOIN avoids the correlated-scalar-
    -- subquery restriction Spark imposes on uncorrelated outer references.
    SELECT
      d.vessel_id, d.shape_id, d.entry_ts, d.last_seen_ts,
      d.last_pos_num, d.n_positions,
      next_pos.event_ts AS exit_ts
    FROM derived d
    LEFT JOIN numbered_positions next_pos
      ON  next_pos.vessel_id = d.vessel_id
     AND  next_pos.pos_num   = d.last_pos_num + 1
    """)

    # ---------------------------------------------------------------------
    # 9. Atomic UPDATE + INSERT + DELETE
    # ---------------------------------------------------------------------
    # MERGE + DELETE wrapped in a single BEGIN ATOMIC transaction so the
    # downstream front end never sees an inconsistent intermediate state
    # between the upsert and the orphan cleanup. Requires the target table
    # to have the `delta.feature.catalogManaged` feature — set above.
    s.sql(f"""
    BEGIN ATOMIC

      -- (a+b) MERGE INTO handles UPDATE and INSERT in one statement.
      --       Spark SQL doesn't support `UPDATE ... FROM <joined-table>`;
      --       MERGE is the canonical update-with-join pattern.
      MERGE INTO {callings_tbl} AS c
      USING callings_from_diff AS n
      ON  c.vessel_id = n.vessel_id
      AND c.shape_id  = n.shape_id
      AND c.entry_ts  = n.entry_ts
      WHEN MATCHED THEN UPDATE SET
          last_seen_ts = n.last_seen_ts,
          exit_ts      = n.exit_ts,
          n_positions  = n.n_positions,
          as_of_ts     = current_timestamp()
      WHEN NOT MATCHED THEN INSERT (
          vessel_id, shape_id, entry_ts, last_seen_ts, exit_ts, n_positions, as_of_ts
      ) VALUES (
          n.vessel_id, n.shape_id, n.entry_ts, n.last_seen_ts, n.exit_ts,
          n.n_positions, current_timestamp()
      );

      -- (c) DELETE orphans inside the affected window that the
      --     re-derivation no longer produces.
      DELETE FROM {callings_tbl}
       WHERE EXISTS (
           SELECT 1 FROM vessel_lookback l
            WHERE l.vessel_id = {callings_tbl}.vessel_id
              AND {callings_tbl}.entry_ts >= l.lookback_ts)
         AND NOT EXISTS (
           SELECT 1 FROM callings_from_diff n
            WHERE n.vessel_id = {callings_tbl}.vessel_id
              AND n.shape_id  = {callings_tbl}.shape_id
              AND n.entry_ts  = {callings_tbl}.entry_ts);

    END
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Start the streaming query

# COMMAND ----------

query = (spark.readStream
    .option("ignoreDeletes", "true")  # tolerate TRUNCATE on silver
    .table(silver)
    .writeStream
    .queryName("callings_stream_v2")
    .foreachBatch(merge_micro_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(processingTime=f"{trigger_seconds} seconds")
    .start()
)
print(f"started callings stream v2: query={query.id}")
query.awaitTermination()
