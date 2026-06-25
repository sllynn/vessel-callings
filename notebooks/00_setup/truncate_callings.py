# Databricks notebook source
# MAGIC %md
# MAGIC # 00 / truncate_callings — one-shot
# MAGIC
# MAGIC Empties `callings_gold` AND `position_shape_matches` without touching
# MAGIC their schemas or the streaming checkpoint. Use to clear stale state
# MAGIC from a prior derivation pass so the next stream restart re-populates
# MAGIC cleanly.

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "marineintel")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

tables = [
    f"{catalog}.{schema}.callings_gold",
    f"{catalog}.{schema}.position_shape_matches",
]

result = []
for t in tables:
    try:
        before = spark.sql(f"SELECT count(*) FROM {t}").collect()[0][0]
        spark.sql(f"TRUNCATE TABLE {t}")
        after = spark.sql(f"SELECT count(*) FROM {t}").collect()[0][0]
        result.append({"table": t, "rows_before": before, "rows_after": after})
    except Exception as e:
        result.append({"table": t, "error": str(e)[:200]})

summary = {"truncated": result}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
