# Databricks notebook source
# MAGIC %md
# MAGIC # 00 / reset_streaming_state
# MAGIC
# MAGIC Drops the four streaming tables (`bronze_ais_positions`,
# MAGIC `silver_positions`, `position_shape_matches`, `callings_gold`) and
# MAGIC clears all streaming checkpoints under the landing volume. Does NOT
# MAGIC touch the static reference tables (`shapes_raw`, `shape_cells`).
# MAGIC
# MAGIC Run this once before starting a fresh streaming demo.

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "clarksons")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

streaming_tables = [
    f"{catalog}.{schema}.bronze_ais_positions",
    f"{catalog}.{schema}.silver_positions",
    f"{catalog}.{schema}.position_shape_matches",
    f"{catalog}.{schema}.callings_gold",
]

checkpoint_root = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints"  # covers v1 + v2
schema_root     = f"/Volumes/{catalog}/{schema}/{volume}/_schema"

# COMMAND ----------

dropped = []
for t in streaming_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        dropped.append(t)
    except Exception as e:
        print(f"  drop {t} → {e}")

cleared = []
for p in (checkpoint_root, schema_root):
    try:
        dbutils.fs.rm(p, recurse=True)
        cleared.append(p)
    except Exception as e:
        print(f"  rm {p} → {e}")

summary = {"dropped_tables": dropped, "cleared_paths": cleared}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
