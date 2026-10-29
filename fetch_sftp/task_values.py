# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Task values
# MAGIC
# MAGIC Place JSON settings files in `fetch_sftp/settings/` and each remote file will be fetched
# MAGIC into the inbox by the fetch_loop task. If you rename a file to something other than `.json`
# MAGIC it will be disabled.
# MAGIC
# MAGIC - Convention: `<source>.<name>.json`<br>
# MAGIC - Example: `partner.export.json`
# MAGIC
# MAGIC Job parameters:
# MAGIC - `catalog_prefix`: picks the inbox, `/Volumes/<catalog_prefix>utility/raw/inbox/`
# MAGIC - `environment`: settings only run when their `"environments"` list includes this
# MAGIC
# MAGIC See `doc/jobs/fetch_sftp.md`.

# COMMAND ----------

import sys
from pathlib import Path

# lakehouse/ is at the repo root, one folder up from this notebook.
sys.path.insert(0, str(Path.cwd().parent))

# COMMAND ----------

import re

from lakehouse.core.config import runs_in
from lakehouse.core.utils import read_json_and_decode

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("catalog_prefix")
environment    = dbutils.widgets.get("environment")

REQUIRED = ("host", "host_key", "username", "secret_scope", "secret_key", "remote_file", "inbox_name")


def check(path, data):
    """Fail on a missing or malformed key, in every file, so a mistake fails in free first."""
    missing = [k for k in REQUIRED if not data.get(k)]
    if missing:
        raise ValueError(f"{path}: missing required key(s) {missing}")
    if not re.fullmatch(r"\S+ [A-Za-z0-9+/]+=*", data["host_key"].strip()):
        raise ValueError(f'{path}: "host_key" must be "<type> <base64>", e.g. from ssh-keyscan -t ed25519 <host>')
    if not data["remote_file"].startswith("/"):
        raise ValueError(f'{path}: "remote_file" must be an absolute path, e.g. /outbound/export.csv')
    inbox_name = data["inbox_name"]
    if "/" in inbox_name or "\\" in inbox_name:
        raise ValueError(f'{path}: "inbox_name" must be a file name, e.g. partner_export.csv')


fetch_task_values = []
skipped = []
for path in sorted(Path("settings").iterdir()):
    if path.is_file() and path.name.endswith(".json"):
        data = read_json_and_decode(path, catalog_prefix)
        check(path, data)
        if runs_in(data, environment, path):
            fetch_task_values.append({"settings_file": str(path), "catalog_prefix": catalog_prefix})
        else:
            skipped.append(str(path))

print(f"environment={environment!r}  catalog_prefix={catalog_prefix!r}")
print(f"To fetch: {len(fetch_task_values)}")
for item in fetch_task_values:
    print(f"   {item['settings_file']}")
print(f"Skipped (not in environment {environment!r}): {len(skipped)}")
for path in skipped:
    print(f"   {path}")

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "fetch", value = fetch_task_values)
