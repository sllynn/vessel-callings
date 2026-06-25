-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 90 / perf_microbench
-- MAGIC
-- MAGIC Lightweight performance sanity-check for the join. Times the
-- MAGIC compute_callings query at a few scales and reports throughput,
-- MAGIC core/boundary ratio, and skew metrics.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'stuart';
CREATE WIDGET TEXT schema DEFAULT 'marineintel_demo';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TODO
-- MAGIC
-- MAGIC 1. Time `compute_callings` over the last 1h / 6h / 24h windows.
-- MAGIC 2. Report:
-- MAGIC    - rows in / rows out
-- MAGIC    - % of matches that took the `core` fast path vs the `ST_Contains` chip path
-- MAGIC    - per-cell distribution (Singapore / Rotterdam / Houston-equivalent will dominate; quantify)
-- MAGIC    - photon stats (peak memory, shuffle bytes)
