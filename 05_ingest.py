# Databricks notebook source
import importlib
import json

from framework.core.utils import read_json_and_decode

# Variables
skip            = dbutils.widgets.get("skip")
stop_here       = dbutils.widgets.get("stop_here")
color           = dbutils.widgets.get("color")
task_value      = json.loads(dbutils.widgets.get("task_value"))
settings        = read_json_and_decode(task_value["settings_file"])

# Skip or stop
if skip == "True":
    dbutils.notebook.exit("Skipping per Workflow task value.")
if stop_here == "True":
    raise Exception("Stop here per task value.")

function_path = settings["function_path"]
module_path, fn_name = function_path.rsplit(".", 1)
function = getattr(importlib.import_module(module_path), fn_name)

# Call ingest function
function(settings)
