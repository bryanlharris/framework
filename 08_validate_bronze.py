# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Validate bronze table for exact duplicate rows. If duplicates are found,
# MAGIC raise an error listing the offending files and recovery steps.
# MAGIC
# MAGIC Tables with `"validate_bronze": false` in their settings are skipped.
# MAGIC See `doc/notebooks.txt`.

# COMMAND ----------

import json

from lakehouse.core.utils import read_json_and_decode

# Workflow parameters and task values
task_config     = json.loads(dbutils.widgets.get("task_config"))
full_table_name = task_config['full_table_name']
settings        = read_json_and_decode(task_config["settings_file"])

# COMMAND ----------

if not settings.get("validate_bronze", True):
    dbutils.notebook.exit(f"Skipped: validate_bronze is false for {full_table_name}")

# COMMAND ----------

df = spark.read.table(full_table_name)

total_count    = df.count()
distinct_count = df.distinct().count()

if total_count != distinct_count:
    duplicate_rows = df.exceptAll(df.distinct())
    duplicate_file_info = (
        duplicate_rows
        .select("source_metadata.file_path", "source_metadata.file_modification_time")
        .distinct()
        .orderBy("source_metadata.file_modification_time")
        .collect()
    )
    files_str = "\n  ".join([
        f"{row['file_path']}  (modified: {row['file_modification_time']})"
        for row in duplicate_file_info
    ])

    history          = spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1").collect()
    current_version  = history[0]["version"] if history else None
    prev_version     = current_version - 1 if current_version is not None else "<previous_version>"
    # +1 = the RESTORE commit, +2 = first new bronze version after the re-run
    starting_version = current_version + 2 if current_version is not None else "<post-restore version + 1>"

    raise ValueError(
        f"Duplicate rows detected in {full_table_name}. "
        f"Found {total_count - distinct_count} duplicate row(s).\n\n"
        f"Source files containing duplicates:\n  {files_str}\n\n"
        "Recovery steps:\n"
        "  1. Restore the bronze table to its previous version:\n"
        f"     RESTORE TABLE {full_table_name} TO VERSION AS OF {prev_version}\n"
        "  2. Choose one approach for the source files:\n"
        "       a. Copy the file you want to keep to a new filename, then delete all\n"
        "          originals listed above. The bronze checkpoint has never seen the new\n"
        "          filename, so no checkpoint or modifiedAfter change is needed.\n"
        "       b. Delete the unwanted file(s) listed above. Then clear the bronze\n"
        "          checkpoint and set modifiedAfter in the bronze settings to just before\n"
        "          the modification time of the file you want to keep (shown above).\n"
        "  3. In the silver readStream_options, add:\n"
        f'       "ignoreDeletes": true\n'
        f'       "startingVersion": {starting_version}\n'
        "  4. Clear the silver checkpoint.\n"
        "  5. Re-run the job.\n"
        "  6. After a successful run:\n"
        "       - Remove startingVersion from the silver settings.\n"
        "       - If you used option 2b, remove modifiedAfter from the bronze settings."
    )
