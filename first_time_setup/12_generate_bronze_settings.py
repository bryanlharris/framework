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
# MAGIC # Generate bronze settings
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC 1. Clear state & outputs (Escape, 0, 0, Enter).
# MAGIC 1. Run all above this cell (Shift-Alt-Up).
# MAGIC 1. Choose a pipeline.
# MAGIC 1. Run all below this cell (Shift-Alt-Down).
# MAGIC
# MAGIC ## WARNING
# MAGIC
# MAGIC Run this if you want to overwrite the bronze settings json files.
# MAGIC
# MAGIC * check bottom of script to print versus overwrite
# MAGIC * check script contents for what this is going to do (you need to code it out yourself)

# COMMAND ----------

# Variables
catalog                 = spark.catalog.currentCatalog()
pipeline                = dbutils.widgets.get("pipeline")
this_pipeline           = settings["pipelines"][pipeline]
bronze_schema_name      = this_pipeline["bronze_schema_name"]
tables                  = this_pipeline["bronze"]["tables"]
custom_sql_commands     = this_pipeline["bronze"]["custom_sql_commands"]
path_glob_map           = this_pipeline["bronze"]["path_glob_map"]
settings                = {}

# COMMAND ----------

for table_name, path_glob in path_glob_map.items():
    settings[table_name] = {
        "bronze_function": "bronze_function",
        "dst_table_name": f"{catalog}.{bronze_schema_name}.{table_name}",
        "file_name": path_glob,
        "readStream_load": f"/Volumes/{catalog}/{bronze_schema_name}/landing/data_files/",
        "readStreamOptions": {
            "header": "true",
            "quote": '"',
            "escape": '"',
            "delimiter": ",",
            "pathGlobFilter": path_glob,
            "encoding": "ISO-8859-1",
            "cloudFiles.format": "csv",
            "cloudFiles.inferColumnTypes": "false",
            "cloudFiles.inferSchema": "false",
            "cloudFiles.schemaLocation": f"/Volumes/{catalog}/{bronze_schema_name}/utility/{table_name}/_schema/",
            "cloudFiles.schemaEvolutionMode": "none",
            "cloudFiles.useNotifications": "false",
            "cloudFiles.useIncrementalListing": "auto",
            "cloudFiles.validateOptions": "true",
            "badRecordsPath": f"/Volumes/{catalog}/{bronze_schema_name}/utility/{table_name}/_badRecords/"
            },
        "writeStream_format": "delta",
        "writeStreamOptions": {
            "mergeSchema": "true",
            "checkpointLocation": f"/Volumes/{catalog}/{bronze_schema_name}/utility/{table_name}/_checkpoints/",
            "delta.columnMapping.mode": "name"
        },
        "writeStream_outputMode": "append"
    }

    if bronze_schema_name == "nmls_mcr_raw":
        settings[table_name]["readStreamOptions"]["multiLine"] = "true"
        settings[table_name]["readStreamOptions"]["ignoreLeadingWhiteSpace"] = "true"
        settings[table_name]["readStreamOptions"]["ignoreTrailingWhiteSpace"] = "true"
        settings[table_name]["readStreamOptions"]["treatEmptyValuesAsNulls"] = "true"
        settings[table_name]["readStreamOptions"]["encoding"] = "UTF-8"

    if bronze_schema_name == "nmls_msb_raw":
        settings[table_name]["readStreamOptions"]["delimiter"] = "|"
        settings[table_name]["readStreamOptions"]["encoding"] = "UTF-8"

    import json
    print(json.dumps(settings[table_name], indent=4))
    # encode_and_save_as_json(settings[table_name], f"/Workspace/EDA/{pipeline}/settings/bronze/{table_name}.json")