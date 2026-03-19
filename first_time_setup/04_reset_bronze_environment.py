# Databricks notebook source
# MAGIC %md ### Prep (read instructions below)

# COMMAND ----------

# MAGIC %run ../functions/utility

# COMMAND ----------

# Reset widgets
dbutils.widgets.removeAll()

# COMMAND ----------

settings = build_dictionary("settings")
pipelines = list(settings["pipelines"].keys())

# COMMAND ----------

# Drop-down for pipeline
dbutils.widgets.dropdown(
    name="pipeline",
    defaultValue="",
    choices=[ "" ] + pipelines
)

# COMMAND ----------

dbutils.notebook.exit("Stop here.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Reset bronze environment
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC 1. Clear state & outputs (Escape, 0, 0, Enter).
# MAGIC 1. Run all above this cell (Select this cell & press Shift-Alt-Up).
# MAGIC 1. Choose a pipeline.
# MAGIC 1. Run all below this cell (Select this cell & press Shift-Alt-Down).
# MAGIC
# MAGIC ## WARNING
# MAGIC
# MAGIC Run this if you want to start from scratch.
# MAGIC
# MAGIC * drop **ALL** bronze tables in the settings
# MAGIC * delete **EVERYTHING** in the landing zone
# MAGIC * delete **EVERYTHING** (except _schema folders) in the utility folder
# MAGIC * create the transaction_history table in bronze
# MAGIC * create the file_version_history table in bronze

# COMMAND ----------

# Variables
catalog                 = spark.catalog.currentCatalog()
pipeline                = dbutils.widgets.get("pipeline")
this_pipeline           = settings["pipelines"][pipeline]
bronze_schema_name      = this_pipeline["bronze_schema_name"]
tables                  = this_pipeline["bronze"]["tables"]
custom_sql_commands     = this_pipeline["bronze"]["custom_sql_commands"]

# COMMAND ----------

# Drop/create all tables
for table_name in tables:
    sql_commands = f"""
        DROP TABLE IF EXISTS {catalog}.{bronze_schema_name}.{table_name};
        CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema_name}.{table_name};
        ALTER TABLE {catalog}.{bronze_schema_name}.{table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
        ALTER TABLE {catalog}.{bronze_schema_name}.{table_name} SET OWNER TO `admins`;
    """

    run_sql_commands(sql_commands, dry_run=False)

# Delete everything under landing folder
folders = getCmd(f"find /Volumes/{catalog}/{bronze_schema_name}/landing -mindepth 1 -maxdepth 1").split('\n')
for folder in folders:
    if folder:
        print(folder)
        dbutils.fs.rm(folder, recurse=True)

# Delete everything at utility/* except for _schema folders
folders = getCmd(f"find /Volumes/{catalog}/{bronze_schema_name}/utility -mindepth 2 -maxdepth 2 -not -path '*/_schema'").split('\n')
for folder in folders:
    if folder:
        print(folder)
        dbutils.fs.rm(folder, recurse=True)

# ALTER the transaction_history table schema (must be specified in settings)
# ALTER the file_version_history table schema (must be specifed in settings)
from textwrap import dedent
sql_commands = dedent(f"""
    ALTER TABLE {catalog}.{bronze_schema_name}.transaction_history
    add columns (
        primary_key string,
        timestamp timestamp,
        userName string,
        job struct<jobId:string,jobName:string,jobRunId:string,runId:string,jobOwnerId:string,triggerType:string>,
        operationParameters map<string,string>,
        readVersion bigint,
        version bigint,
        userId string,
        operation string,
        isBlindAppend boolean,
        engineInfo string,
        operationMetrics map<string,string>,
        isolationLevel string,
        table_name string,
        notebook struct<notebookId:string>,
        clusterId string,
        userMetadata string
    );
    ALTER TABLE {catalog}.{bronze_schema_name}.file_version_history
    add columns (
        primary_key STRING,
        file_path ARRAY<STRING>
    );
    """.strip("\n"))

run_sql_commands(sql_commands, dry_run=False)

run_sql_commands(custom_sql_commands, dry_run=False)