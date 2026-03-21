# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This logs a little bit of history information for all versions of each table (meaning all versions that still exist after vacuum etc).

# COMMAND ----------

import json

# Workflow parameters and task settings
pipeline            = dbutils.widgets.get("pipeline")
skip                = dbutils.widgets.get("skip")
stop_here           = dbutils.widgets.get("stop_here")
task_settings       = json.loads(dbutils.widgets.get("task_settings"))
full_table_name     = task_settings['full_table_name']

# COMMAND ----------

# Skip if True (continues remaining workflow)
if skip == "True":
    dbutils.notebook.exit("Skipping per Workflow task settings.")

# Stop if True (stops and does not continue workflow)
if stop_here == "True":
    raise Exception("Stop here per task settings.")

# COMMAND ----------

from pyspark.sql import functions as F

# Variables
catalog_name = full_table_name.split(".")[0]
schema_name = full_table_name.split(".")[1]

# Figure out bronze schema name
if schema_name.endswith("_raw"):
    bronze_schema = schema_name
else:
    bronze_schema = schema_name + "_raw"

# Table to write to
transaction_table_name = f"{catalog_name}.{bronze_schema}.transaction_history"

# COMMAND ----------

# Put table_name, history, and primary_key into df
df = (
    spark.sql(f"describe history {full_table_name}")
    .withColumn("table_name", F.lit(full_table_name))
    .withColumn('primary_key', F.expr(' coalesce(table_name, "") || "_" || coalesce(cast(version as string), "") '))
    .selectExpr("table_name", "* except (table_name)")
    )

# Merge
df.createOrReplaceTempView("df")
spark.sql(f"""
            merge WITH SCHEMA EVOLUTION into {transaction_table_name} as target
            using df as source
            on target.primary_key = source.primary_key
            when matched then update set *
            when not matched then insert *
          """)