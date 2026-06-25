# Databricks notebook source
# MAGIC %md
# MAGIC # 20 / index_positions — bronze → silver streaming
# MAGIC
# MAGIC The generator writes directly to `bronze_ais_positions`. This notebook
# MAGIC runs a single Delta-streaming query: `bronze → silver_positions` with
# MAGIC `h3_longlatash3` enrichment and a lat/lon validity filter, on a
# MAGIC `processingTime` trigger. Liquid Clustered on `(h3_cell, event_ts)`.
# MAGIC
# MAGIC Set `reset_state=true` on first run of the day to drop silver and
# MAGIC clear the streaming checkpoint so the stream re-derives from bronze.

# COMMAND ----------

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "marineintel")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("position_res", "8")
dbutils.widgets.text("trigger_seconds", "30")
dbutils.widgets.dropdown("reset_state", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
position_res = int(dbutils.widgets.get("position_res"))
trigger_seconds = int(dbutils.widgets.get("trigger_seconds"))
reset_state = dbutils.widgets.get("reset_state") == "true"

bronze = f"{catalog}.{schema}.bronze_ais_positions"
silver = f"{catalog}.{schema}.silver_positions"
silver_checkpoint = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints/silver_positions"

print(f"bronze            : {bronze}")
print(f"silver            : {silver}  (position_res={position_res})")
print(f"silver_checkpoint : {silver_checkpoint}")
print(f"trigger           : {trigger_seconds}s")
print(f"reset_state       : {reset_state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset (optional) + ensure silver table exists

# COMMAND ----------

if reset_state:
    print("reset_state=true — dropping silver and clearing checkpoint")
    spark.sql(f"DROP TABLE IF EXISTS {silver}")
    try:
        dbutils.fs.rm(silver_checkpoint, recurse=True)
    except Exception as e:
        print(f"  rm({silver_checkpoint}) → {e}")

# Ensure bronze exists before we readStream from it — avoids the race where
# this notebook starts before the generator's pip install completes and bronze
# hasn't been created yet.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze} (
  mmsi        BIGINT,
  vessel_id   STRING,
  event_ts    TIMESTAMP,
  ingest_ts   TIMESTAMP,
  lon         DOUBLE,
  lat         DOUBLE,
  sog         DOUBLE,
  cog         DOUBLE,
  heading     DOUBLE,
  nav_status  STRING,
  vessel_type STRING,
  vessel_name STRING
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver} (
  mmsi        BIGINT,
  vessel_id   STRING,
  event_ts    TIMESTAMP,
  ingest_ts   TIMESTAMP,
  lon         DOUBLE,
  lat         DOUBLE,
  sog         DOUBLE,
  cog         DOUBLE,
  heading     DOUBLE,
  nav_status  STRING,
  vessel_type STRING,
  vessel_name STRING,
  h3_cell     BIGINT
)
CLUSTER BY (h3_cell, event_ts)
TBLPROPERTIES (
  -- Required so silver can participate in the downstream callings stream's
  -- BEGIN ATOMIC transaction (which reads from silver via views).
  'delta.feature.catalogManaged' = 'supported'
)
""")

# Belt-and-brace: if the table already existed without the feature (created
# by an earlier version of this notebook), ALTER it now. Idempotent.
spark.sql(f"""
ALTER TABLE {silver}
SET TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream bronze → silver (with h3_cell)

# COMMAND ----------

silver_query = (spark.readStream
    .option("ignoreDeletes", "true")  # tolerate TRUNCATE on bronze
    .table(bronze)
    .selectExpr(
        "mmsi",
        "vessel_id",
        "event_ts",
        "ingest_ts",
        "lon", "lat", "sog", "cog", "heading",
        "nav_status", "vessel_type", "vessel_name",
        f"h3_longlatash3(lon, lat, {position_res}) AS h3_cell",
    )
    .where("lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180")
    # No `.withWatermark` — silver is a stateless enrich+filter sink, no
    # stateful operator downstream that would consume the watermark.
    .writeStream
    .queryName("bronze_to_silver")
    .option("checkpointLocation", silver_checkpoint)
    .trigger(processingTime=f"{trigger_seconds} seconds")
    .toTable(silver)
)
print(f"started silver stream: query={silver_query.id}")

# COMMAND ----------

silver_query.awaitTermination()
