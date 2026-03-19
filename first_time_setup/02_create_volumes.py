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
# MAGIC # Create volumes
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC 1. Run all cells above this cell.
# MAGIC 1. Select a pipeline from the widget.
# MAGIC 1. Run all cells below this cell.

# COMMAND ----------

# Variables
catalog = spark.catalog.currentCatalog()
pipeline = dbutils.widgets.get("pipeline")

# Sanity check
if pipeline == "":
    raise Exception(f"""
                    Error: Select a valid pipeline to run this script.
                    Pipeline selected was: "{pipeline}".
                    """)

# COMMAND ----------

# Bronze landing zone
run_sql_commands((f"""
                    CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{pipeline}_raw.landing LOCATION "s3://cfpb-stgint-datadev-landing-zone/{pipeline}_raw";
                    ALTER VOLUME {catalog}.{pipeline}_raw.landing SET OWNER TO `admins`;
                    GRANT ALL PRIVILEGES  ON VOLUME {catalog}.{pipeline}_raw.landing TO `data-architecture`;
                    GRANT ALL PRIVILEGES  ON VOLUME {catalog}.{pipeline}_raw.landing TO `data-engineering`;
                    GRANT ALL PRIVILEGES  ON VOLUME {catalog}.{pipeline}_raw.landing TO `data-operations`;
                  """),
                 dry_run=False
                 )

# Bronze utility
run_sql_commands((f"""
                    CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{pipeline}_raw.utility LOCATION "s3://cfpb-stgint-datadev-databricks-utility/{pipeline}_raw";
                    ALTER VOLUME {catalog}.{pipeline}_raw.utility SET OWNER TO `admins`;
                    GRANT ALL PRIVILEGES  ON VOLUME {catalog}.{pipeline}_raw.utility TO `data-architecture`;
                    GRANT ALL PRIVILEGES  ON VOLUME {catalog}.{pipeline}_raw.utility TO `data-engineering`;
                    GRANT ALL PRIVILEGES  ON VOLUME {catalog}.{pipeline}_raw.utility TO `data-operations`;
                  """),
                 dry_run=False
                 )

# Silver utility
run_sql_commands((f"""
                    CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{pipeline}.utility LOCATION "s3://cfpb-stgint-datadev-databricks-utility/{pipeline}";
                    ALTER VOLUME {catalog}.{pipeline}.utility SET OWNER TO `admins`;
                    GRANT ALL PRIVILEGES  ON VOLUME {catalog}.{pipeline}.utility TO `data-architecture`;
                    GRANT ALL PRIVILEGES  ON VOLUME {catalog}.{pipeline}.utility TO `data-engineering`;
                    GRANT ALL PRIVILEGES  ON VOLUME {catalog}.{pipeline}.utility TO `data-operations`;
                  """),
                 dry_run=False
                 )