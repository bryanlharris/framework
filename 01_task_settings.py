# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Framework v0.04.

# COMMAND ----------

# MAGIC %run ./functions/utility

# COMMAND ----------

datasync_task_settings               = read_json_and_decode(f"settings/*task_settings/*datasync_settings.json")
bronze_task_settings                 = read_json_and_decode(f"settings/*task_settings/*bronze_settings.json")
silver_task_settings                 = read_json_and_decode(f"settings/*task_settings/*silver_settings.json")
gold_task_settings                   = read_json_and_decode(f"settings/*task_settings/*gold_settings.json")
file_version_history_task_settings   = read_json_and_decode(f"settings/*task_settings/*file_version_history_settings.json")
transaction_history_task_settings    = read_json_and_decode(f"settings/*task_settings/*transaction_history_settings.json")

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "datasync",               value = datasync_task_settings)
dbutils.jobs.taskValues.set(key = "bronze",                 value = bronze_task_settings)
dbutils.jobs.taskValues.set(key = "silver",                 value = silver_task_settings)
dbutils.jobs.taskValues.set(key = "gold",                   value = gold_task_settings)
dbutils.jobs.taskValues.set(key = "file_version_history",   value = file_version_history_task_settings)
dbutils.jobs.taskValues.set(key = "transaction_history",    value = transaction_history_task_settings)