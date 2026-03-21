# Databricks notebook source
import json

from framework.registry import function_registries
from framework.core.utils import read_json_and_decode

# Variables
skip            = dbutils.widgets.get("skip")
stop_here       = dbutils.widgets.get("stop_here")
color           = dbutils.widgets.get("color")
task_settings   = json.loads(dbutils.widgets.get("task_settings"))
settings        = read_json_and_decode(task_settings["settings_file"])

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
