# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Task values
# MAGIC
# MAGIC Place JSON settings files in `ingest/settings/bronze/` or `ingest/settings/silver/` and their settings
# MAGIC will be passed to the correct task. If you rename a file to something other than `.json`
# MAGIC it will be disabled.
# MAGIC
# MAGIC - Convention: `<catalog>.<table>.json` (the folder gives the schema)<br>
# MAGIC - Example: `fred.fedfunds.json`
# MAGIC
# MAGIC Job parameters:
# MAGIC - `catalog_prefix`: replaces `${catalog_prefix}` in settings (e.g. `dev_`, empty in prod)
# MAGIC - `environment`: settings with an `"environments"` list only run when it includes this
# MAGIC
# MAGIC Look in `doc/examples` for examples.

# COMMAND ----------

import sys
from pathlib import Path

# lakehouse/ is at the repo root, one folder up from this notebook.
sys.path.insert(0, str(Path.cwd().parent))

# COMMAND ----------

from lakehouse.core.config import scan_settings_folder, extract_destination_tables, skipped_settings

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("catalog_prefix")
environment    = dbutils.widgets.get("environment")

bronze_task_values        = scan_settings_folder("bronze", catalog_prefix=catalog_prefix, environment=environment)
silver_task_values        = scan_settings_folder("silver", dependents=False, catalog_prefix=catalog_prefix, environment=environment)
silver_dep_task_values    = scan_settings_folder("silver", dependents=True, catalog_prefix=catalog_prefix, environment=environment)
bronze_tables_task_values = extract_destination_tables("bronze", catalog_prefix=catalog_prefix, environment=environment)

skipped = skipped_settings("bronze", catalog_prefix, environment) + skipped_settings("silver", catalog_prefix, environment)
print(f"environment={environment!r}  catalog_prefix={catalog_prefix!r}")
print(f"Skipped (not in environment {environment!r}): {len(skipped)}")
for path in skipped:
    print(f"   {path}")

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "bronze",        value = bronze_task_values)
dbutils.jobs.taskValues.set(key = "silver",        value = silver_task_values)
dbutils.jobs.taskValues.set(key = "silver_dep",    value = silver_dep_task_values)
dbutils.jobs.taskValues.set(key = "bronze_tables", value = bronze_tables_task_values)
