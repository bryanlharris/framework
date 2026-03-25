# Databricks notebook source
import importlib
import json

from lakehouse.core.utils import read_json_and_decode

# Variables
task_config         = json.loads(dbutils.widgets.get("task_config"))
settings            = read_json_and_decode(task_config["settings_file"])

function_path = settings["function_path"]
module_path, fn_name = function_path.rsplit(".", 1)
function = getattr(importlib.import_module(module_path), fn_name)

# Call ingest function
function(spark, settings)
