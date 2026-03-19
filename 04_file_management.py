# Databricks notebook source
import json

# Workflow parameters and task settings
pipeline            = dbutils.widgets.get("pipeline")
skip                = dbutils.widgets.get("skip")
stop_here           = dbutils.widgets.get("stop_here")
config              = dbutils.widgets.get("config")
env_name            = dbutils.widgets.get("env_name")
group               = dbutils.widgets.get("group")
project_name        = dbutils.widgets.get("project_name")
schema_name         = dbutils.widgets.get("schema_name")
version             = dbutils.widgets.get("version")

# Skip if True (continues remaining workflow)
if skip == "True":
    dbutils.notebook.exit("Skipping per Workflow task settings.")

# Stop if True (stops and does not continue workflow)
if stop_here == "True":
    raise Exception("Stop here per task settings.")

# COMMAND ----------

# Set up values for the %run command below
dbutils.widgets.text(name="config",         defaultValue=config)
dbutils.widgets.text(name="env_name",       defaultValue=env_name)
dbutils.widgets.text(name="group",          defaultValue=group)
dbutils.widgets.text(name="project_name",   defaultValue=project_name)
dbutils.widgets.text(name="schema_name",    defaultValue=schema_name)
dbutils.widgets.text(name="version",        defaultValue=version)

# COMMAND ----------

# MAGIC %run /Workspace/EDA/file_management/run_notebook