# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Framework v0.04.

# COMMAND ----------

# MAGIC %run ./functions/utility

# COMMAND ----------

def read_task_settings_json(workspace_path):
    import json
    import os
    import base64
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import workspace

    client = WorkspaceClient()
    response = client.workspace.export(
        path=f"{os.getcwd()}/{workspace_path}",
        format=workspace.ExportFormat.AUTO,
    )

    return json.loads(base64.b64decode(response.content).decode("utf-8"))

# COMMAND ----------

datasync_task_settings               = read_task_settings_json("settings/00_task_settings/01_datasync_settings.json")
bronze_task_settings                 = read_task_settings_json("settings/00_task_settings/02_bronze_settings.json")
silver_task_settings                 = read_task_settings_json("settings/00_task_settings/03_silver_settings.json")
gold_task_settings                   = read_task_settings_json("settings/00_task_settings/04_gold_settings.json")
file_version_history_task_settings   = read_task_settings_json("settings/00_task_settings/05_file_version_history_settings.json")
transaction_history_task_settings    = read_task_settings_json("settings/00_task_settings/06_transaction_history_settings.json")

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "datasync",               value = datasync_task_settings)
dbutils.jobs.taskValues.set(key = "bronze",                 value = bronze_task_settings)
dbutils.jobs.taskValues.set(key = "silver",                 value = silver_task_settings)
dbutils.jobs.taskValues.set(key = "gold",                   value = gold_task_settings)
dbutils.jobs.taskValues.set(key = "file_version_history",   value = file_version_history_task_settings)
dbutils.jobs.taskValues.set(key = "transaction_history",    value = transaction_history_task_settings)
