# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Validate bronze table for exact duplicate rows. If duplicates are found,
# MAGIC raise a ValueError listing the offending file paths and recovery steps.

# COMMAND ----------

import json

# Workflow parameters and task values
task_config     = json.loads(dbutils.widgets.get("task_config"))
full_table_name = task_config['full_table_name']

# COMMAND ----------

df = spark.read.table(full_table_name)

total_count    = df.count()
distinct_count = df.distinct().count()

if total_count != distinct_count:
    duplicate_rows = df.exceptAll(df.distinct())
    duplicate_file_paths = (
        duplicate_rows
        .select("source_metadata.file_path")
        .distinct()
        .rdd.flatMap(lambda r: r)
        .collect()
    )
    paths_str = "\n  ".join(duplicate_file_paths)
    raise ValueError(
        f"Duplicate rows detected in {full_table_name}. "
        f"Found {total_count - distinct_count} duplicate row(s).\n\n"
        f"Source files containing duplicates:\n  {paths_str}\n\n"
        "Recovery steps:\n"
        "  1. Delete the unwanted duplicate source file(s) listed above.\n"
        "  2. Restore the bronze table to its previous version using Delta time travel:\n"
        f"     RESTORE TABLE {full_table_name} TO VERSION AS OF <previous_version>\n"
        "  3. Clear the bronze and silver checkpoints.\n"
        "  4. Drop both the bronze and silver tables.\n"
        "  5. Re-run the job."
    )
