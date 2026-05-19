-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 90 / correctness_oracle
-- MAGIC
-- MAGIC Brute-force ground truth: for a small sample of vessel positions,
-- MAGIC compute callings via direct `ST_Contains(shape.geom, point)` against
-- MAGIC `shapes_raw` and compare against the H3-indexed result in
-- MAGIC `callings_gold`. Differences are the signal — should be zero on
-- MAGIC matched (vessel × shape × event_ts) triples.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'stuart';
CREATE WIDGET TEXT schema DEFAULT 'clarksons_demo';
CREATE WIDGET TEXT sample_n DEFAULT '10000';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TODO
-- MAGIC
-- MAGIC 1. Sample `${sample_n}` rows from `silver_positions`.
-- MAGIC 2. Cross-join with `shapes_raw`, evaluate `ST_Contains(s.geom, ST_Point(p.lon, p.lat, 4326))`.
-- MAGIC 3. Compare against the H3-pipeline callings for the same vessel × event_ts.
-- MAGIC 4. Surface any disagreement — false positives (chip miss-classified) or false negatives
-- MAGIC    (boundary chip rejected a true containment).
