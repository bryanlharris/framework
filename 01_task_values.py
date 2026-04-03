# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Task values
# MAGIC
# MAGIC Place JSON settings files in `settings/bronze/` or `settings/silver/` and their settings
# MAGIC will be passed to the correct task. If you rename a file to something other than `.json`
# MAGIC it will be disabled.
# MAGIC
# MAGIC - Convention: `<catalog>.<schema>.<table>.json`<br>
# MAGIC - Example: `demo.bronze.fedfunds.json`
# MAGIC
# MAGIC Look in `doc/examples` for examples.

# COMMAND ----------

from lakehouse.core.config import scan_settings_folder, extract_destination_tables

# COMMAND ----------

bronze_task_values               = scan_settings_folder("bronze")
silver_task_values               = scan_settings_folder("silver", dependents=False)
silver_dep_task_values           = scan_settings_folder("silver", dependents=True)
file_version_history_task_values = extract_destination_tables("bronze")
transaction_history_task_values  = extract_destination_tables("bronze") + extract_destination_tables("silver")
bronze_tables_task_values        = extract_destination_tables("bronze")

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "bronze",               value = bronze_task_values)
dbutils.jobs.taskValues.set(key = "silver",               value = silver_task_values)
dbutils.jobs.taskValues.set(key = "silver_dep",           value = silver_dep_task_values)
dbutils.jobs.taskValues.set(key = "file_version_history", value = file_version_history_task_values)
dbutils.jobs.taskValues.set(key = "transaction_history",  value = transaction_history_task_values)
dbutils.jobs.taskValues.set(key = "bronze_tables",        value = bronze_tables_task_values)
