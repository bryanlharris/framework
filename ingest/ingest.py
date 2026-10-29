# Databricks notebook source
import importlib
import json
import sys
from pathlib import Path

# lakehouse/ is at the repo root, one folder up from this notebook.
sys.path.insert(0, str(Path.cwd().parent))

from lakehouse.core.utils import read_json_and_decode

# Variables
task_config         = json.loads(dbutils.widgets.get("task_config"))
settings            = read_json_and_decode(task_config["settings_file"], task_config.get("catalog_prefix", ""))

function_path = settings["function_path"]
module_path, fn_name = function_path.rsplit(".", 1)
function = getattr(importlib.import_module(module_path), fn_name)

# Call ingest function
function(spark, settings)
