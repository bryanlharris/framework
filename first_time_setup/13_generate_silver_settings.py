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
# MAGIC # Silver tables settings
# MAGIC
# MAGIC ## Prerequisites/Requirements
# MAGIC
# MAGIC Before you can run this script, take the following actions.
# MAGIC
# MAGIC * Run the pipeline up to bronze but no further (you can set the silver task settings in the GUI to stop early).
# MAGIC * Run `05_reset_silver_tables`.
# MAGIC * Requires a real cluster because it connects to the on-premise stuff (not serverless).
# MAGIC * Requires a bronze table on databricks to fuzzy search column names from.
# MAGIC
# MAGIC ## WARNING
# MAGIC
# MAGIC Run this if you want to overwrite the silver settings json files. It fuzzy searches column names on databricks and PPAS/Citus and tries to build a column map.
# MAGIC
# MAGIC * check bottom of script to print versus overwrite
# MAGIC * there is no customization of the settings like in bronze
# MAGIC * it's trying to figure out column names and data types
# MAGIC
# MAGIC ### After Actions
# MAGIC
# MAGIC After running this notebook check the json settings to see if the column mappings are correct.
# MAGIC
# MAGIC ### PS
# MAGIC
# MAGIC This is the one time I got fuzzy searching to work. I found fuzzy searching extremely prone to errors so be sure and check the results.

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

# MAGIC %pip install rapidfuzz

# COMMAND ----------

import json
import re

# Variables
settings = {}
databricks_bronze_schema_name = f"{pipeline}_raw"
databricks_silver_schema_name = f"{pipeline}"

# Create a dictionary for this schema
settings[databricks_silver_schema_name] = {}
settings[databricks_silver_schema_name]["table_settings"] = {}

# List of tables from each schema
bronze_tables = [table.name for table in spark.catalog.listTables(databricks_bronze_schema_name)]
silver_tables = [table.name for table in spark.catalog.listTables(databricks_silver_schema_name)]

# Skip certain tables
tables_to_ignore = [ "transaction_history", "file_version_history", "meta_source_files", "file_sync_history" ]
bronze_tables = [table for table in bronze_tables if table not in tables_to_ignore]
silver_tables = [table for table in silver_tables if table not in tables_to_ignore]

# Put in some basic silver settings
for table in bronze_tables:
    settings[databricks_silver_schema_name]["table_settings"][table] = {
        "src_table_name": f"{catalog}.{databricks_bronze_schema_name}.{table}",
        "dst_table_name": f"{catalog}.{databricks_silver_schema_name}.{table}"
    }

###########################################################################
# This section creates a column_map to use to rename the columns.
# Bronze and silver must already exist.
# Silver is a deep clone of DMS.
# Bronze is a load of the CSV files
###########################################################################

# Columns to ignore
remove_databricks_bronze_columns = [ "ingest_time", "source_metadata", "row_checksum", "rescued_data" ]
remove_databricks_silver_columns = [
    "id",
    "file_id",
    f"{pipeline}_file_id",
    "source_metadata",
    "file_path",
    "file_modification_time",
    "legacy01",
    "legacy02"
]

# Columns are (usually) in same order
for table in bronze_tables:
    if table in silver_tables:
        databricks_bronze_columns = [col.name for col in spark.catalog.listColumns(f"{databricks_bronze_schema_name}.{table}")]
        databricks_silver_columns = [col.name for col in spark.catalog.listColumns(f"{databricks_silver_schema_name}.{table}")]

        # Remove columns to be ignored
        databricks_bronze_columns = [col for col in databricks_bronze_columns if col not in remove_databricks_bronze_columns]
        databricks_silver_columns = [col for col in databricks_silver_columns if col not in remove_databricks_silver_columns]

        # Create column map and lost columns
        column_map = {}

        # Put them in the dictionary
        column_map = dict(zip(databricks_bronze_columns, databricks_silver_columns))

        # I only use the fuzzy map if the side-by-side doesn't work
        # Otherwise I leave it in the settings but ignore it
        ###########################################################################
        # Fuzzy column mapping starts here
        ###########################################################################
        from rapidfuzz import process, fuzz
        from rapidfuzz.utils import default_process

        def preprocess_column(col):
            prefixes = ['is_', '_']
            for prefix in prefixes:
                if col.lower().startswith(prefix):
                    col = col[len(prefix):]
            col = col.lower().replace('_', '')
            return col

        def match_columns(source_cols, target_cols):
            matches = []
            used_targets = set()

            for source in source_cols:
                processed_source = preprocess_column(source)
                available_targets = [t for t in target_cols if t not in used_targets]
                match = process.extractOne(
                    processed_source,
                    available_targets,
                    scorer=fuzz.WRatio,
                    processor=preprocess_column,
                    score_cutoff=75
                )

                if match:
                    target = match[0]
                    used_targets.add(target)
                    matches.append((source, target))

            arranged_source = [m[0] for m in matches]
            arranged_target = [m[1] for m in matches]

            return arranged_source, arranged_target

        matched_bronze, matched_silver = match_columns(databricks_bronze_columns, databricks_silver_columns)
        fuzzy_column_map = dict(zip(matched_bronze, matched_silver))
        ###########################################################################
        # Fuzzy column mapping ends here
        ###########################################################################

        # Save column map and fuzzy column map to the settings dictionary
        settings[databricks_silver_schema_name]["table_settings"][table]["column_map"] = column_map
        settings[databricks_silver_schema_name]["table_settings"][table]["fuzzy_column_map"] = fuzzy_column_map

        # Drop columns that don't or shouldn't exist in silver
        columns_to_drop = [col for col in databricks_bronze_columns if col not in fuzzy_column_map.keys()]
        settings[databricks_silver_schema_name]["table_settings"][table]["columns_to_drop"] = columns_to_drop

for table in bronze_tables:
    databricks_silver_df = spark.table(f"{databricks_silver_schema_name}.{table}")
    databricks_silver_schema = {
        field.name: f"decimal({field.dataType.precision},{field.dataType.scale})"
        if field.dataType.typeName() == "decimal"
        else field.dataType.typeName() for field in databricks_silver_df.schema.fields
    }

    # The data types are what we got from silver
    data_type_map = {key: value for key, value in databricks_silver_schema.items() if key not in remove_databricks_silver_columns}

    # Save the data_type_map into the settings dictionary
    settings[databricks_silver_schema_name]["table_settings"][table]["data_type_map"] = data_type_map

for table in settings[databricks_silver_schema_name]["table_settings"].keys():
    # encode_and_save_as_json(settings[databricks_silver_schema_name]["table_settings"][table], f"/Workspace/EDA/{pipeline}/settings/silver/{table}.json")
    print(json.dumps(settings[databricks_silver_schema_name]["table_settings"][table], indent=4))