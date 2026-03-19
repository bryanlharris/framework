# Databricks notebook source
# MAGIC %md ### Prep (read instructions below)

# COMMAND ----------

# MAGIC %run ../functions/utility

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

settings = build_dictionary("settings")
pipelines = list(settings["pipelines"].keys())

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
# MAGIC # Copy Into Schema
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC 1. Clear state & outputs (Escape, 0, 0, Enter).
# MAGIC 1. Run all above this cell (Select this cell & press Shift-Alt-Up).
# MAGIC 1. Choose a pipeline.
# MAGIC 1. Run all below this cell (Select this cell & press Shift-Alt-Down).

# COMMAND ----------

# MAGIC %run ../functions/common

# COMMAND ----------

import json

settings = build_dictionary("settings")
pipeline = dbutils.widgets.get("pipeline")

# COMMAND ----------

# Variables
catalog = spark.catalog.currentCatalog()
this_pipeline = settings["pipelines"][pipeline]

for copy_into in settings["pipelines"][pipeline]["copy_into"]:
    # Variables
    old_schema_name = copy_into["old_schema_name"]
    migration_volume_name = copy_into["migration_volume_name"]
    schema_name = copy_into["schema_name"]

    # Drop and reload tables
    for table in this_pipeline["silver"]["tables_in_dms"]:
        custom_tables = [t["table_name"] for t in this_pipeline["silver"]["tables_with_custom_types"]]
        if table not in custom_tables:
            sql_commands = f"""
                DROP TABLE IF EXISTS {catalog}.{schema_name}.{table};

                CREATE TABLE {catalog}.{schema_name}.{table};

                COPY INTO {catalog}.{schema_name}.{table}
                FROM "/Volumes/{catalog}/migration/{migration_volume_name}/{old_schema_name}/{table}/"
                FILEFORMAT = PARQUET
                COPY_OPTIONS ('mergeSchema' = 'true');

                ALTER TABLE {catalog}.{schema_name}.{table} SET OWNER TO admins;
                ALTER TABLE {catalog}.{schema_name}.{table} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
            """

            run_sql_commands(sql_commands)

    # Drop and reload tables with custom types
    for table_dict in this_pipeline["silver"]["tables_with_custom_types"]:
        # Variables
        table_name = table_dict["table_name"]
        view_name = table_dict["view_name"]
        columns = [c["name"] for c in table_dict["columns"]]
        types = [c["type"] for c in table_dict["columns"]]
        cols_expr = ", ".join(columns)
        cast_expr_list = []
        for col, _type in zip(columns, types):
            cast_expr_list.append(f"""
                                    CASE 
                                        WHEN ABS(CAST({col} AS {_type})) < 0E-10 THEN NULL
                                        ELSE CAST({col} AS {_type})
                                    END as {col}""")
        cast_expr = ",\n                ".join(cast_expr_list)

        # SQL
        sql_commands = f"""
            DROP TABLE IF EXISTS {catalog}.{schema_name}.{table_name};

            CREATE TABLE {catalog}.{schema_name}.{table_name};

            COPY INTO {catalog}.{schema_name}.{table_name}
            FROM (
                select *
                except ({cols_expr}),
                {cast_expr}
                from "/Volumes/{catalog}/migration/{migration_volume_name}/{old_schema_name}/{view_name}/"
            )
            FILEFORMAT = PARQUET
            COPY_OPTIONS ('mergeSchema' = 'true');

            ALTER TABLE {catalog}.{schema_name}.{table_name} SET OWNER TO admins;
            ALTER TABLE {catalog}.{schema_name}.{table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
        """

        run_sql_commands(sql_commands)