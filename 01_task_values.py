# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Task values

# COMMAND ----------

from lakehouse.core.utils import read_json_and_decode

# COMMAND ----------

task_values_root = "settings/task_values"

bronze_task_values                 = read_json_and_decode(f"{task_values_root}/02_bronze.json")
silver_task_values                 = read_json_and_decode(f"{task_values_root}/03_silver.json")
gold_task_values                   = read_json_and_decode(f"{task_values_root}/04_gold.json")
file_version_history_task_values   = read_json_and_decode(f"{task_values_root}/05_file_version_history.json")
transaction_history_task_values    = read_json_and_decode(f"{task_values_root}/06_transaction_history.json")

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "bronze",                 value = bronze_task_values)
dbutils.jobs.taskValues.set(key = "silver",                 value = silver_task_values)
dbutils.jobs.taskValues.set(key = "gold",                   value = gold_task_values)
dbutils.jobs.taskValues.set(key = "file_version_history",   value = file_version_history_task_values)
dbutils.jobs.taskValues.set(key = "transaction_history",    value = transaction_history_task_values)
