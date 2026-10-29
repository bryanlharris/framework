# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Task values
# MAGIC
# MAGIC Place JSON settings files in `fetch_url/settings/` and each URL will be fetched into
# MAGIC the inbox by the fetch_loop task. If you rename a file to something other than `.json`
# MAGIC it will be disabled.
# MAGIC
# MAGIC - Convention: `<source>.<name>.json`<br>
# MAGIC - Example: `fred.fedfunds.json`
# MAGIC
# MAGIC Job parameters:
# MAGIC - `catalog_prefix`: picks the inbox, `/Volumes/<catalog_prefix>utility/raw/inbox/`
# MAGIC - `environment`: settings only run when their `"environments"` list includes this
# MAGIC
# MAGIC See `doc/jobs/fetch_url.md`.

# COMMAND ----------

import sys
from pathlib import Path

# lakehouse/ is at the repo root, one folder up from this notebook.
sys.path.insert(0, str(Path.cwd().parent))

# COMMAND ----------

from lakehouse.core.config import runs_in
from lakehouse.core.utils import read_json_and_decode

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("catalog_prefix")
environment    = dbutils.widgets.get("environment")


def check(path, data):
    """Fail on a bad url or inbox_name, in every file, so a mistake fails in free first."""
    url = data.get("url", "")
    inbox_name = data.get("inbox_name", "")
    if not str(url).startswith("https://"):
        raise ValueError(f'{path}: "url" is required and must start with https://')
    if not inbox_name or "/" in inbox_name or "\\" in inbox_name:
        raise ValueError(f'{path}: "inbox_name" is required and must be a file name, e.g. fred_fedfunds.csv')
    gunzip = data.get("gunzip", False)
    if not isinstance(gunzip, bool):
        raise ValueError(f'{path}: "gunzip" must be true or false')
    if gunzip and inbox_name.endswith(".gz"):
        raise ValueError(f'{path}: with "gunzip", inbox_name is the unzipped name, e.g. edsm_powerPlay.json')


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
