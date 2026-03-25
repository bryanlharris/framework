# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This logs a little bit of history information for all versions of each table (meaning all versions that still exist after vacuum etc).

# COMMAND ----------

import json

# Workflow parameters and task values
task_config         = json.loads(dbutils.widgets.get("task_config"))
full_table_name     = task_config['full_table_name']

# COMMAND ----------

from pyspark.sql import functions as F
from lakehouse.core.utils import ensure_table_exists

# Variables
catalog_name = full_table_name.split(".")[0]
schema_name = full_table_name.split(".")[1]
transaction_table_name = f"{catalog_name}.bronze.transaction_history"

# COMMAND ----------

# Put table_name, history, and primary_key into df
df = (
    spark.sql(f"describe history {full_table_name}")
    .withColumn("table_name", F.lit(full_table_name))
    .withColumn('primary_key', F.expr(' coalesce(table_name, "") || "_" || coalesce(cast(version as string), "") '))
    .selectExpr("table_name", "* except (table_name)")
    )

# Ensure target table exists before merge
schema_string = ", ".join(f"`{name}` {dtype}" for name, dtype in df.dtypes)
ensure_table_exists(spark, transaction_table_name, schema_string)

# Merge
df.createOrReplaceTempView("df")
spark.sql(f"""
            merge into {transaction_table_name} as target
            using df as source
            on target.primary_key = source.primary_key
            when matched then update set *
            when not matched then insert *
          """)
