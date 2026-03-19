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
# MAGIC # Create the schemas
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC 1. Run all cells above this cell.
# MAGIC 1. Select a pipeline from the widget.
# MAGIC 1. Run all cells below this cell.
# MAGIC 1. End.

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

sql_commands = (f"""
                CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{pipeline}_dms`;
                CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{pipeline}_raw`;
                CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{pipeline}`;

                ALTER SCHEMA `{catalog}`.`{pipeline}_dms`   OWNER TO `admins`;
                ALTER SCHEMA `{catalog}`.`{pipeline}_raw`   OWNER TO `admins`;
                ALTER SCHEMA `{catalog}`.`{pipeline}`       OWNER TO `admins`;

                GRANT SELECT          ON SCHEMA `{catalog}`.`{pipeline}_dms` TO `data-world`;
                GRANT USE SCHEMA      ON SCHEMA `{catalog}`.`{pipeline}_dms` TO `data-world`;
                GRANT ALL PRIVILEGES  ON SCHEMA `{catalog}`.`{pipeline}_dms` TO `data-architecture`;
                GRANT ALL PRIVILEGES  ON SCHEMA `{catalog}`.`{pipeline}_dms` TO `data-engineering`;
                GRANT ALL PRIVILEGES  ON SCHEMA `{catalog}`.`{pipeline}_dms` TO `data-operations`;

                GRANT SELECT          ON SCHEMA `{catalog}`.`{pipeline}_raw` TO `data-world`;
                GRANT USE SCHEMA      ON SCHEMA `{catalog}`.`{pipeline}_raw` TO `data-world`;
                GRANT ALL PRIVILEGES  ON SCHEMA `{catalog}`.`{pipeline}_raw` TO `data-architecture`;
                GRANT ALL PRIVILEGES  ON SCHEMA `{catalog}`.`{pipeline}_raw` TO `data-engineering`;
                GRANT ALL PRIVILEGES  ON SCHEMA `{catalog}`.`{pipeline}_raw` TO `data-operations`;

                GRANT SELECT          ON SCHEMA `{catalog}`.`{pipeline}`     TO `data-world`;
                GRANT USE SCHEMA      ON SCHEMA `{catalog}`.`{pipeline}`     TO `data-world`;
                GRANT ALL PRIVILEGES  ON SCHEMA `{catalog}`.`{pipeline}`     TO `data-architecture`;
                GRANT ALL PRIVILEGES  ON SCHEMA `{catalog}`.`{pipeline}`     TO `data-engineering`;
                GRANT ALL PRIVILEGES  ON SCHEMA `{catalog}`.`{pipeline}`     TO `data-operations`;
                """)

run_sql_commands(sql_commands)