# Databricks notebook source
# MAGIC %md
# MAGIC # 90 / check_status — one-shot table counts + freshness probe
# MAGIC
# MAGIC Quick read across the streaming tables for poll-style validation.
# MAGIC Exits with a JSON summary visible via `databricks jobs get-run-output`.

# COMMAND ----------

import json
import os

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "marineintel")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

landing_dir = f"/Volumes/{catalog}/{schema}/{volume}/ais"

def safe_count(tbl: str) -> dict:
    try:
        row = spark.sql(f"SELECT count(*) AS n FROM {tbl}").collect()[0]
        out = {"rows": row["n"]}
        try:
            ts_row = spark.sql(
                f"SELECT min(event_ts) AS mn, max(event_ts) AS mx FROM {tbl}"
            ).collect()[0]
            out["min_event_ts"] = str(ts_row["mn"]) if ts_row["mn"] else None
            out["max_event_ts"] = str(ts_row["mx"]) if ts_row["mx"] else None
        except Exception:
            pass
        return out
    except Exception as e:
        return {"error": str(e)[:200]}

summary = {
    "landing_files": len([f for f in os.listdir(landing_dir) if f.endswith(".jsonl")]) if os.path.isdir(landing_dir) else 0,
    "bronze_ais_positions":     safe_count(f"{catalog}.{schema}.bronze_ais_positions"),
    "silver_positions":         safe_count(f"{catalog}.{schema}.silver_positions"),
    "position_shape_matches":   safe_count(f"{catalog}.{schema}.position_shape_matches"),
    "callings_gold":            safe_count(f"{catalog}.{schema}.callings_gold"),
}
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
