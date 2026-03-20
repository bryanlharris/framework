# Databricks notebook source
import json

# Treat `functions/` as a normal package via `functions/__init__.py`.
# Proof of concept: utility helpers now use normal package imports, while
# transformation notebooks still load dynamically through `import_notebook(...)`.
from functions.utility import getCmd, import_notebook, read_json_and_decode

# Keep `commonFunctions.py`, `ingestFunctions.py`, and
# `standardTransformations.py` on dynamic loading for now so Databricks can
# continue resolving them from the workspace before we attempt a broader migration.
files = getCmd(f"[ -d functions ] && find functions -mindepth 1 -maxdepth 1 -type f -and ! -name utility").split('\n')
for file in files:
    if file:
        import_notebook(file)

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

# User `functions/*/*.function.py` notebooks still load dynamically through
# `import_notebook(...)` and register callables in `globals()`, which is how
# settings-driven lookups continue to resolve them.
files = getCmd(f"find functions/*{color} -type f -regex '.*\.function\(s?\)'").split('\n')
for file in files:
    if file:
        import_notebook(file)

# Call ingest function
if callable(globals()[settings[f"{color}_function"]]):
    globals()[settings[f"{color}_function"]](settings)
else:
    raise Exception(f"Could not find {color} ingest function name in settings.")
