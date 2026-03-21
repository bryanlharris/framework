# Databricks notebook source
import json

from framework.registry import function_registries
from framework.spark_utils import read_json_and_decode

# Variables
pipeline        = dbutils.widgets.get("pipeline")
skip            = dbutils.widgets.get("skip")
stop_here       = dbutils.widgets.get("stop_here")
color           = dbutils.widgets.get("color")
task_settings   = json.loads(dbutils.widgets.get("task_settings"))
full_table_name = task_settings['full_table_name']
catalog_name    = full_table_name.split(".")[0]
schema_name     = full_table_name.split(".")[1]
table           = full_table_name.split(".")[2]
settings        = read_json_and_decode(f"settings/{color}/{table}.json")

# Skip or stop
if skip == "True":
    dbutils.notebook.exit("Skipping per Workflow task settings.")
if stop_here == "True":
    raise Exception("Stop here per task settings.")

function_name = settings[f"{color}_function"]
function_registry = function_registries.get(color, {})
function = function_registry.get(function_name)

# Call ingest function
if callable(function):
    function(settings)
else:
    raise Exception(f"Could not find {color} ingest function name in settings.")
