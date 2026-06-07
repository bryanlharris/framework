# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Validate each bronze landing zone before ingestion. For every settings file in
# MAGIC `settings/bronze/`, assert that exactly one new (unread) file is queued and that
# MAGIC file contains no duplicate rows. All errors are collected and raised together.

# COMMAND ----------

import json
import fnmatch
from pathlib import Path

# COMMAND ----------

def _strip_dbfs(p):
    return p[len("dbfs:"):] if p.startswith("dbfs:") else p

errors = []

for settings_path in sorted(Path("settings/bronze").iterdir()):
    if not settings_path.is_file() or settings_path.suffix != ".json":
        continue

    with settings_path.open() as f:
        settings = json.load(f)

    readStream_options = settings.get("readStream_options", {})

    if "cloudFiles.format" not in readStream_options:
        errors.append(f"{settings_path.name}: missing readStream_options.cloudFiles.format")
        continue

    readStream_path   = settings["readStream_path"]
    destination_table = settings["destination_table"]
    glob_filter       = readStream_options.get("pathGlobFilter", "*")

    try:
        all_files = [
            f.path
            for f in dbutils.fs.ls(readStream_path)
            if fnmatch.fnmatch(f.name, glob_filter)
        ]
    except Exception:
        continue

    if spark.catalog.tableExists(destination_table):
        ingested = {
            _strip_dbfs(row["file_path"])
            for row in (
                spark.read.table(destination_table)
                .select("source_metadata.file_path")
                .distinct()
                .collect()
            )
        }
    else:
        ingested = set()

    unread = [p for p in all_files if _strip_dbfs(p) not in ingested]

    if len(unread) == 0:
        continue
    elif len(unread) > 1:
        names = ", ".join(p.split("/")[-1] for p in unread)
        errors.append(
            f"{destination_table}: {len(unread)} unread files (expected 1): {names}"
        )
        continue

    file_path  = unread[0]
    fmt        = readStream_options["cloudFiles.format"]
    batch_opts = {k: v for k, v in readStream_options.items() if not k.startswith("cloudFiles.")}

    df             = spark.read.format(fmt).options(**batch_opts).load(file_path)
    total_count    = df.count()
    distinct_count = df.distinct().count()

    if total_count != distinct_count:
        filename = file_path.split("/")[-1]
        errors.append(
            f"{destination_table}: {filename} contains "
            f"{total_count - distinct_count} duplicate row(s)"
        )

if errors:
    raise ValueError("Landing zone validation failed:\n  " + "\n  ".join(errors))
