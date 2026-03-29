# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Task values

# COMMAND ----------

from lakehouse.core.config import scan_settings_folder, extract_destination_tables

# COMMAND ----------

bronze_task_values               = scan_settings_folder("bronze")
silver_task_values               = scan_settings_folder("silver")
file_version_history_task_values = extract_destination_tables("bronze")
transaction_history_task_values  = extract_destination_tables("bronze") + extract_destination_tables("silver")
bronze_tables_task_values        = extract_destination_tables("bronze")

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "bronze",               value = bronze_task_values)
dbutils.jobs.taskValues.set(key = "silver",               value = silver_task_values)
dbutils.jobs.taskValues.set(key = "file_version_history", value = file_version_history_task_values)
dbutils.jobs.taskValues.set(key = "transaction_history",  value = transaction_history_task_values)
dbutils.jobs.taskValues.set(key = "bronze_tables",        value = bronze_tables_task_values)
