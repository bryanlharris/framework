# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Call one REST API and write its response into the inbox, as `inbox_name`. file_router
# MAGIC routes it from there and the ingest job loads it. Runs once per settings file, in the
# MAGIC fetch_loop task. See `doc/jobs/fetch_rest.md`.

# COMMAND ----------

import json
import sys
from datetime import datetime
from pathlib import Path

# lakehouse/ is at the repo root, one folder up from this notebook.
sys.path.insert(0, str(Path.cwd().parent))

import requests

from lakehouse.core.utils import read_json_and_decode

# Workflow parameters and task values
task_config    = json.loads(dbutils.widgets.get("task_config"))
catalog_prefix = task_config.get("catalog_prefix", "")
settings_file  = task_config["settings_file"]
settings       = read_json_and_decode(settings_file, catalog_prefix)

url          = settings["url"]
method       = settings.get("method", "GET")
records_path = settings.get("records_path")
record_key   = settings.get("record_key")
destination  = Path(f"/Volumes/{catalog_prefix}utility/raw/inbox") / settings["inbox_name"]
timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")

# COMMAND ----------

def read_secrets(entries):
    """{"name": {"scope", "key", "prefix"}} -> {"name": prefix + secret value}."""
    return {
        name: s.get("prefix", "") + dbutils.secrets.get(scope=s["scope"], key=s["key"])
        for name, s in entries.items()
    }


print("==================================================")
print(f"  FETCH_REST REPORT — {timestamp}")
print(f"  {settings_file}")
print("==================================================")

secret_values = []
try:
    secret_params  = read_secrets(settings.get("secret_params", {}))
    secret_headers = read_secrets(settings.get("secret_headers", {}))
    secret_values  = [v for v in [*secret_params.values(), *secret_headers.values()] if v]
    response = requests.request(
        method,
        url,
        params  = {**settings.get("params", {}), **secret_params},
        headers = {**settings.get("headers", {}), **secret_headers},
        json    = settings.get("body"),
        timeout = 300,
    )
    response.raise_for_status()
    # Build the whole file in memory first, so a bad response never leaves
    # a partial file in the inbox for file_router to route.
    if records_path:
        records = response.json()[records_path]
        if record_key:
            records = [record[record_key] for record in records]
        content = "\n".join(json.dumps(record) for record in records).encode("utf-8")
    else:
        records = None
        content = response.content
    destination.write_bytes(content)
except Exception as e:
    # Error text can hold the request URL, query string included, so hide
    # any secret values in it, and raise a copy without the original
    # (whose traceback would show them).
    message = f"{type(e).__name__}: {e}"
    for value in secret_values:
        message = message.replace(value, "***")
    print("\n👎👎👎 Failed")
    print(f"   {method} {url}")
    print(f"   {message}")
    print("\n==================================================")
    raise RuntimeError(message) from None

print("\n👍👍👍 Fetched")
print(f"   {method} {url} → {destination}")
print(f"   {len(content):,} bytes" + (f", {len(records):,} records" if records is not None else ""))
print("\n==================================================")
