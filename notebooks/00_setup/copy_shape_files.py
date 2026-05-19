# Databricks notebook source
# MAGIC %md
# MAGIC # 00 / copy_shape_files
# MAGIC
# MAGIC Copies the three shape parquet files and the display-only shipping-lanes
# MAGIC GeoJSON from the deployed bundle's workspace files area into the UC
# MAGIC Volume that downstream notebooks read from. Idempotent: skips files
# MAGIC that are already in place with matching size.

# COMMAND ----------

import os
import shutil

dbutils.widgets.text("catalog", "stuart")
dbutils.widgets.text("schema", "clarksons")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("workspace_files_path", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
workspace_files_path = dbutils.widgets.get("workspace_files_path")

assert workspace_files_path, "workspace_files_path must be supplied by the bundle (${workspace.file_path})"

source_dir = f"{workspace_files_path}/data"
target_dir = f"/Volumes/{catalog}/{schema}/{volume}/shapes"

files = [
    "marine_regions_global_oceans_seas_v01_simple.parquet",
    "marine_regions_global_eezs_simple.parquet",
    "UKHO_IMO_Routeing_Measures_Areas.parquet",
    "Shipping_Lanes_v1.geojson",
]

print(f"source: {source_dir}")
print(f"target: {target_dir}")

# COMMAND ----------

os.makedirs(target_dir, exist_ok=True)

for fname in files:
    src = os.path.join(source_dir, fname)
    dst = os.path.join(target_dir, fname)

    if not os.path.exists(src):
        raise FileNotFoundError(f"expected source file not present: {src}")

    src_size = os.path.getsize(src)

    if os.path.exists(dst) and os.path.getsize(dst) == src_size:
        print(f"  skip {fname} ({src_size:,} bytes, already in place)")
        continue

    print(f"  copy {fname} ({src_size:,} bytes)")
    shutil.copy(src, dst)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify what's now in the volume

# COMMAND ----------

for entry in sorted(os.listdir(target_dir)):
    full = os.path.join(target_dir, entry)
    if os.path.isfile(full):
        print(f"  {os.path.getsize(full):>12,}  {entry}")
