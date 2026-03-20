# Databricks notebook source
import json

from functions.commonFunctions import *
from functions.ingestFunctions import *
from functions.standardTransformations import *
from functions.bronze import bronze_function_to_delta_table, example_1
from functions.utility import getCmd, import_notebook, read_json_and_decode

# Variables
pipeline            = dbutils.widgets.get("pipeline")
skip                = dbutils.widgets.get("skip")
stop_here           = dbutils.widgets.get("stop_here")
color               = dbutils.widgets.get("color")
task_settings       = json.loads(dbutils.widgets.get("task_settings"))
full_table_name     = task_settings['full_table_name']
catalog_name        = full_table_name.split(".")[0]
schema_name         = full_table_name.split(".")[1]
table               = full_table_name.split(".")[2]
settings            = read_json_and_decode(f"settings/{color}/{table}.json")

# Skip or stop
if skip == "True":
    dbutils.notebook.exit("Skipping per Workflow task settings.")
if stop_here == "True":
    raise Exception("Stop here per task settings.")

STATIC_FUNCTIONS = {
    "bronze_function_to_delta_table": bronze_function_to_delta_table,
    "example_1": example_1,
}

function_name = settings[f"{color}_function"]
function = STATIC_FUNCTIONS.get(function_name)

if function is None:
    # User `functions/*/*.function.py` notebooks still load dynamically through
    # `import_notebook(...)` and register callables in `globals()`, which is how
    # settings-driven lookups continue to resolve them.
    files = getCmd(fr"find functions/*{color} -type f -regex '.*\.functions?'").split('\n')
    for file in files:
        if file:
            import_notebook(file)

    function = globals().get(function_name)

# Call ingest function
if callable(function):
    function(settings)
else:
    raise Exception(f"Could not find {color} ingest function name in settings.")
