# Databricks notebook source
# MAGIC %md
# MAGIC # 40 / replay_shape_change
# MAGIC
# MAGIC Demo helper: rewrite a chosen shape (expand an ATBA, redraw a TSS lane)
# MAGIC at the current sim-time. Drives the CDF-fed recompute on
# MAGIC `callings_merge` so the bitemporal correctness story becomes visible
# MAGIC in `callings_gold` (rows re-emit with new `as_of_ts`; prior versions
# MAGIC remain queryable via `valid_to < as_of_ts`).

# COMMAND ----------

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "clarksons_demo")
dbutils.widgets.text("shape_id", "")          # empty = let the notebook pick
dbutils.widgets.text("mutation", "expand_atba")  # expand_atba | shrink_tss | redraw_eez

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
shape_id = dbutils.widgets.get("shape_id") or None
mutation = dbutils.widgets.get("mutation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO
# MAGIC
# MAGIC 1. If `shape_id` is empty, pick one representative for the chosen
# MAGIC    `mutation` type (a Shetland ATBA, a Dover TSS lane, a UK EEZ
# MAGIC    edge).
# MAGIC 2. Compute the perturbed geometry — buffer outward / contract / rotate
# MAGIC    a corner — and UPDATE `shapes_raw` with the new `geom` and a fresh
# MAGIC    `valid_from`.
# MAGIC 3. Trigger `build_shape_cells` for the affected `shape_id` only (or
# MAGIC    full rebuild — small table, cheap).
# MAGIC 4. Log the mutation event with sim-time + before/after areas so the
# MAGIC    callings recompute can be observed in the dashboard.
