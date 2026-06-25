# Databricks notebook source
# MAGIC %md
# MAGIC # 20 / generate_ais — long-running, direct-to-bronze
# MAGIC
# MAGIC Synthetic AIS generator that writes batches of records directly into the
# MAGIC `bronze_ais_positions` Delta table — no intermediate JSONL files, no
# MAGIC Auto Loader. Buffers up to `flush_interval_seconds` of wall-clock or
# MAGIC `flush_buffer_records` rows, whichever comes first, then appends.
# MAGIC
# MAGIC Runs until `max_runtime_minutes` of wall-clock has elapsed. Set
# MAGIC `reset_state=true` on first start of the day to drop bronze and let
# MAGIC the downstream streams rebuild silver / callings_gold from scratch.

# COMMAND ----------

# MAGIC %pip install uv

# COMMAND ----------

# MAGIC %sh uv pip install -r ../../requirements.lock

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "marineintel")
dbutils.widgets.text("seed", "20260519")
dbutils.widgets.text("n_vessels", "500")
dbutils.widgets.text("sim_speedup", "60")
dbutils.widgets.text("lateness_pct", "5")
dbutils.widgets.text("lateness_max_hours", "6")
dbutils.widgets.text("gps_jitter_m", "10")
dbutils.widgets.text("lane_sigma_m", "800")
dbutils.widgets.text("dropout_pct", "0.5")
dbutils.widgets.text("workspace_files_path", "")
dbutils.widgets.text("max_runtime_minutes", "240")
dbutils.widgets.text("flush_interval_seconds", "5")
dbutils.widgets.text("flush_buffer_records", "5000")
dbutils.widgets.dropdown("reset_state", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
workspace_files_path = dbutils.widgets.get("workspace_files_path")
max_runtime_minutes = float(dbutils.widgets.get("max_runtime_minutes"))
seed = int(dbutils.widgets.get("seed"))
n_vessels = int(dbutils.widgets.get("n_vessels"))
sim_speedup = float(dbutils.widgets.get("sim_speedup"))
lateness_pct = float(dbutils.widgets.get("lateness_pct"))
lateness_max_hours = float(dbutils.widgets.get("lateness_max_hours"))
gps_jitter_m = float(dbutils.widgets.get("gps_jitter_m"))
lane_sigma_m = float(dbutils.widgets.get("lane_sigma_m"))
dropout_pct = float(dbutils.widgets.get("dropout_pct"))
flush_interval_s = float(dbutils.widgets.get("flush_interval_seconds"))
flush_buffer_n = int(dbutils.widgets.get("flush_buffer_records"))
reset_state = dbutils.widgets.get("reset_state") == "true"

assert workspace_files_path, "workspace_files_path must be supplied by the bundle (${workspace.file_path})"
ports_yaml = f"{workspace_files_path}/src/generator/ports.yaml"
bronze = f"{catalog}.{schema}.bronze_ais_positions"

print(f"bronze              : {bronze}")
print(f"max_runtime_minutes : {max_runtime_minutes}")
print(f"flush_interval_s    : {flush_interval_s}")
print(f"flush_buffer_n      : {flush_buffer_n}")
print(f"reset_state         : {reset_state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset (optional) + ensure bronze table exists with the right schema

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType, TimestampType,
)

bronze_schema = StructType([
    StructField("mmsi",        LongType()),
    StructField("vessel_id",   StringType()),
    StructField("event_ts",    TimestampType()),
    StructField("ingest_ts",   TimestampType()),
    StructField("lon",         DoubleType()),
    StructField("lat",         DoubleType()),
    StructField("sog",         DoubleType()),
    StructField("cog",         DoubleType()),
    StructField("heading",     DoubleType()),
    StructField("nav_status",  StringType()),
    StructField("vessel_type", StringType()),
    StructField("vessel_name", StringType()),
])

if reset_state:
    print("reset_state=true — dropping bronze_ais_positions")
    spark.sql(f"DROP TABLE IF EXISTS {bronze}")

# Create the empty table up-front so the streaming silver reader has
# something to watch from tick 0.
spark.createDataFrame([], schema=bronze_schema).write.format("delta").mode("append").saveAsTable(bronze)

# COMMAND ----------

import sys
if workspace_files_path not in sys.path:
    sys.path.insert(0, workspace_files_path)

import random
import time
from datetime import datetime, timezone
from src.generator.world import World
from src.generator.routing import RouteCache
from src.generator.fleet import build_fleet
from src.generator.stepper import Stepper
from src.generator.perturb import Perturber

# COMMAND ----------

world = World.load(ports_yaml)
routes = RouteCache()
fleet = build_fleet(seed=seed, n=n_vessels, world=world)
perturb = Perturber(
    rng=random.Random(seed + 1),
    lateness_pct=lateness_pct,
    lateness_max_hours=lateness_max_hours,
    gps_jitter_m=gps_jitter_m,
    lane_sigma_m=lane_sigma_m,
    dropout_pct=dropout_pct,
)
wallclock_start = time.time()
stepper = Stepper(
    world=world, routes=routes, fleet=fleet,
    rng=random.Random(seed + 2),
    sim_start_epoch=wallclock_start,
    sim_speedup=sim_speedup,
    tick_sim_minutes=1.0,
)

by_b = {}
for v in fleet:
    by_b[v.behaviour] = by_b.get(v.behaviour, 0) + 1
print(f"world: {len(world.ports)} ports — fleet: {len(fleet)} vessels")
for b, n in sorted(by_b.items()):
    print(f"  {b:10s} {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous emission — buffered append to bronze
# MAGIC
# MAGIC Records are buffered in-memory until either `flush_interval_seconds`
# MAGIC of wall-clock has passed or `flush_buffer_records` rows have queued,
# MAGIC then appended as a single Delta write. With defaults this is roughly
# MAGIC one append every 5 seconds — small enough latency for the streaming
# MAGIC pipeline to feel live, large enough writes that we don't drown Delta
# MAGIC in tiny files.

# COMMAND ----------

deadline = wallclock_start + max_runtime_minutes * 60
tick_wallclock_seconds = 60.0 / sim_speedup
buffer: list[tuple] = []
last_flush = time.time()
n_written = 0
n_flushes = 0


def flush(buf: list) -> int:
    if not buf:
        return 0
    df = spark.createDataFrame(buf, schema=bronze_schema)
    df.write.format("delta").mode("append").saveAsTable(bronze)
    return len(buf)


try:
    tick_idx = 0
    while time.time() < deadline:
        loop_start = time.time()
        clean = stepper.step()
        for p in perturb.apply(clean):
            r = p.record
            ingest_ts = r.event_ts + p.ingest_offset_s
            buffer.append((
                r.mmsi, r.vessel_id,
                datetime.fromtimestamp(r.event_ts, tz=timezone.utc),
                datetime.fromtimestamp(ingest_ts, tz=timezone.utc),
                r.lon, r.lat, r.sog, r.cog, r.heading,
                r.nav_status, r.vessel_type, r.vessel_name,
            ))
        wall_elapsed = time.time() - last_flush
        if wall_elapsed >= flush_interval_s or len(buffer) >= flush_buffer_n:
            n_written += flush(buffer)
            n_flushes += 1
            buffer = []
            last_flush = time.time()
        # Pace wall-clock to sim-time
        elapsed = time.time() - loop_start
        if elapsed < tick_wallclock_seconds:
            time.sleep(tick_wallclock_seconds - elapsed)
        tick_idx += 1
        if tick_idx % 60 == 0:
            wall_min = (time.time() - wallclock_start) / 60.0
            print(f"  tick={tick_idx}  wall={wall_min:.1f} min  records_written={n_written:,}  flushes={n_flushes}")
finally:
    n_written += flush(buffer)
    n_flushes += 1 if buffer else 0
    print(f"final flush — total records_written={n_written:,}  flushes={n_flushes}")

# COMMAND ----------

import json
summary = {
    "n_vessels":             n_vessels,
    "n_ticks":               tick_idx,
    "n_records_written":     n_written,
    "n_flushes":             n_flushes,
    "wallclock_elapsed_min": (time.time() - wallclock_start) / 60.0,
    "route_cache_legs":      len(routes._cache),
    "behaviour_mix":         by_b,
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
