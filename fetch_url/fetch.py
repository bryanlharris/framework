# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Fetch one URL into the inbox, as `inbox_name`. file_router routes it from there and
# MAGIC the ingest job loads it. Runs once per settings file, in the fetch_loop task.
# MAGIC See `doc/jobs/fetch_url.md`.

# COMMAND ----------

import gzip
import json
import shutil
import sys
import tempfile
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

url         = settings["url"]
gunzip      = settings.get("gunzip", False)
destination = Path(f"/Volumes/{catalog_prefix}utility/raw/inbox") / settings["inbox_name"]
timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")

# COMMAND ----------

print("==================================================")
print(f"  FETCH_URL REPORT — {timestamp}")
print(f"  {settings_file}")
print("==================================================")

# Stream to local disk first, so a big file never sits in memory and the inbox
# (whose trigger starts file_router) only ever sees the finished file.
# gunzip unzips on the way, like the upload scripts do before uploading.
with tempfile.TemporaryDirectory() as tmp:
    download = Path(tmp) / settings["inbox_name"]
    try:
        # Name the job; www.edsm.net answers 403 to requests' default User-Agent.
        with requests.get(url, timeout=300, stream=True, headers={"User-Agent": "fetch_url/1.0"}) as response:
            response.raise_for_status()
            response.raw.decode_content = True
            body = gzip.GzipFile(fileobj=response.raw) if gunzip else response.raw
            with open(download, "wb") as out:
                shutil.copyfileobj(body, out, 1024 * 1024)
        size = download.stat().st_size
        shutil.copyfile(download, destination)
    except Exception as e:
        print("\n👎👎👎 Failed")
        print(f"   {url}")
        print(f"   {type(e).__name__}: {e}")
        print("\n==================================================")
        raise

print("\n👍👍👍 Fetched" + (" and gunzipped" if gunzip else ""))
print(f"   {url} → {destination}")
print(f"   {size:,} bytes")
print("\n==================================================")
