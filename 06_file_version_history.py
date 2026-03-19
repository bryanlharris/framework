# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This creates a table that shows what file(s) went into loading data for each version of the table.

# COMMAND ----------

import json

# Workflow parameters and task settings
pipeline            = dbutils.widgets.get("pipeline")
skip                = dbutils.widgets.get("skip")
stop_here           = dbutils.widgets.get("stop_here")
task_settings       = json.loads(dbutils.widgets.get("task_settings"))
full_table_name     = task_settings['full_table_name']

# COMMAND ----------

# Skip if True (continues remaining workflow)
if skip == "True":
    dbutils.notebook.exit("Skipping per Workflow task settings.")

# Stop if True (stops and does not continue workflow)
if stop_here == "True":
    raise Exception("Stop here per task settings.")

# COMMAND ----------

from pyspark.sql import functions as F
import json

# Variables
catalog_name = full_table_name.split(".")[0]
schema_name = full_table_name.split(".")[1]

# Figure out bronze schema name
if schema_name.endswith("_raw"):
    bronze_schema = schema_name
else:
    bronze_schema = schema_name + "_raw"

# Table to write to
file_version_table_name = f"{catalog_name}.{bronze_schema}.file_version_history"

# COMMAND ----------

from pyspark.sql.functions import col, lit

hist = spark.sql(f"describe history {full_table_name}")

# Get table history
update_or_merge_version_rows = (
    hist.filter((col("operation") == "STREAMING UPDATE") | (col("operation") == "MERGE"))
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

    # Turn on schema evolution (will not work on serverless clusters)
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Merge
    df.createOrReplaceTempView("df")
    spark.sql(f"""
                merge WITH SCHEMA EVOLUTION into {file_version_table_name} as target
                using df as source
                on target.primary_key = source.primary_key
                when matched then update set *
                when not matched then insert *
            """)

# COMMAND ----------

# from pyspark.sql import DataFrame
# from pyspark.sql.functions import lit, col, collect_list, struct

# # Empty df
# df = spark.createDataFrame([], schema="primary_key STRING, file_path: ARRAY<STRING>")

# # Get table history
# all_version_rows = (
#     spark.sql(f"describe history {full_table_name}")
#     .filter((col("operation") == "STREAMING UPDATE") | (col("operation") == "MERGE"))
#     .select("version")
#     .sort("version", ascending=False)
#     .limit(1)
#     .collect()
# )

# # Loop through here
# for row in all_version_rows:
#     version = row["version"]
#     print(version)

#     # Read table as of this version
#     this_version_df = (
#         spark.read
#         .format("delta")
#         .option("versionAsOf", version)
#         .table(full_table_name)
#         .withColumn("primary_key", lit(f"{full_table_name}_{version}"))
#         .withColumn("file_path", col("source_metadata.file_path"))
#         .select("primary_key", "file_path")
#         .dropDuplicates()
#         .groupBy("primary_key")
#         .agg(collect_list("file_path").alias("file_path"))
#         .select("primary_key", "file_path")
#     )

#     # Add to the empty df from above
#     df = df.union(this_version_df)

# # Turn on schema evolution (will not work on serverless clusters)
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# # Merge
# df.createOrReplaceTempView("df")
# spark.sql(f"""
#             merge WITH SCHEMA EVOLUTION into {file_version_table_name} as target
#             using df as source
#             on target.primary_key = source.primary_key
#             when matched then update set *
#             when not matched then insert *
#           """)