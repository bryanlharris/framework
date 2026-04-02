# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This creates a table that shows what file(s) went into loading data for each version of the table.

# COMMAND ----------

import json

# Workflow parameters and task values
task_config         = json.loads(dbutils.widgets.get("task_config"))
full_table_name     = task_config['full_table_name']

# COMMAND ----------

from pyspark.sql import functions as F

# Variables
catalog_name = full_table_name.split(".")[0]
schema_name = full_table_name.split(".")[1]
file_version_table_name = f"{catalog_name}.bronze.file_version_history"

# COMMAND ----------

from pyspark.sql.functions import col, lit
from lakehouse.core.utils import ensure_table_exists
from datetime import datetime, timedelta, timezone
import re

hist = spark.sql(f"describe history {full_table_name}")

# Get actual deletedFileRetentionDuration from table properties
tbl_props = {row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {full_table_name}").collect()}
retention_str = tbl_props.get("delta.deletedFileRetentionDuration", "interval 168 hours")

# Parse "interval N unit" — convert to hours
match = re.match(r"interval\s+(\d+)\s+(\w+)", retention_str)
if match:
    value, unit = int(match.group(1)), match.group(2).lower()
    unit_to_hours = {"hours": 1, "hour": 1, "days": 24, "day": 24, "weeks": 168, "week": 168}
    retention_hours = value * unit_to_hours.get(unit, 24)
else:
    retention_hours = 168  # fallback to Delta default

retention_cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)

# Get table history
update_or_merge_version_rows = (
    hist.filter((col("operation") == "STREAMING UPDATE") | (col("operation") == "MERGE"))
    .filter(col("timestamp") >= retention_cutoff)
    .select("version")
    .distinct()
    .collect()
)
version_list = [row["version"] for row in update_or_merge_version_rows]
version_list = sorted(version_list)
prev_files = set()
file_version_history_records = []

# Loop through relevant versions here
for version in version_list:
    this_version_df = (
        spark.read
        .format("delta")
        .option("versionAsOf", version)
        .table(full_table_name)
        .select(col("source_metadata.file_path").alias("file_path"))
        .dropDuplicates()
    )
    file_path_list = this_version_df.collect()
    file_path_list = [row.file_path for row in file_path_list]
    new_files = set(file_path_list) - prev_files

    # Keep a set of all prev_files as we go up to higher versions
    prev_files.update(new_files)
    if len(new_files) > 0:
        file_version_history_records.append((f"{full_table_name}_{version}", list(new_files)))

# Only do this if we have records to append (save time I hope)
if len(file_version_history_records) > 0:
    # Create df
    df = spark.createDataFrame(file_version_history_records, "primary_key STRING, file_path ARRAY<STRING>")
    df.createOrReplaceTempView("df")

    # Ensure target table exists before merge
    ensure_table_exists(spark, file_version_table_name, "primary_key STRING, file_path ARRAY<STRING>")

    # Merge
    df.createOrReplaceTempView("df")
    spark.sql(f"""
                merge into {file_version_table_name} as target
                using df as source
                on target.primary_key = source.primary_key
                when matched then update set *
                when not matched then insert *
            """)
