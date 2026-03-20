# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Task settings

# COMMAND ----------

# MAGIC %run ./functions/utility

# COMMAND ----------

task_settings_root = "settings/00_task_settings"

bronze_task_settings                 = read_json_and_decode(f"{task_settings_root}/02_bronze_settings.json")
silver_task_settings                 = read_json_and_decode(f"{task_settings_root}/03_silver_settings.json")
gold_task_settings                   = read_json_and_decode(f"{task_settings_root}/04_gold_settings.json")
file_version_history_task_settings   = read_json_and_decode(f"{task_settings_root}/05_file_version_history_settings.json")
transaction_history_task_settings    = read_json_and_decode(f"{task_settings_root}/06_transaction_history_settings.json")

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "bronze",                 value = bronze_task_settings)
dbutils.jobs.taskValues.set(key = "silver",                 value = silver_task_settings)
dbutils.jobs.taskValues.set(key = "gold",                   value = gold_task_settings)
dbutils.jobs.taskValues.set(key = "file_version_history",   value = file_version_history_task_settings)
dbutils.jobs.taskValues.set(key = "transaction_history",    value = transaction_history_task_settings)
