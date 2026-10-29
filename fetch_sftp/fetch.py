# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Fetch one file from an SFTP server into the inbox, as `inbox_name`. file_router routes
# MAGIC it from there and the ingest job loads it. Runs once per settings file, in the
# MAGIC fetch_loop task. See `doc/jobs/fetch_sftp.md`.

# COMMAND ----------

import base64
import io
import json
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

# lakehouse/ is at the repo root, one folder up from this notebook.
sys.path.insert(0, str(Path.cwd().parent))

import paramiko

from lakehouse.core.utils import read_json_and_decode

# Workflow parameters and task values
task_config    = json.loads(dbutils.widgets.get("task_config"))
catalog_prefix = task_config.get("catalog_prefix", "")
settings_file  = task_config["settings_file"]
settings       = read_json_and_decode(settings_file, catalog_prefix)

host        = settings["host"]
port        = settings.get("port", 22)
remote_file = settings["remote_file"]
destination = Path(f"/Volumes/{catalog_prefix}utility/raw/inbox") / settings["inbox_name"]
timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")

# COMMAND ----------

def load_private_key(text):
    """The private key from the secret; RSA, Ed25519 or ECDSA."""
    for key_class in (paramiko.Ed25519Key, paramiko.ECDSAKey, paramiko.RSAKey):
        try:
            return key_class.from_private_key(io.StringIO(text))
        except paramiko.SSHException:
            continue
    raise ValueError("private key in the secret is not a readable RSA, Ed25519 or ECDSA key")


def load_host_key(line):
    """The server's public key from a "<type> <base64>" line, as ssh-keyscan prints it."""
    key_type, key_b64 = line.split()[:2]
    return paramiko.PKey.from_type_string(key_type, base64.b64decode(key_b64))


print("==================================================")
print(f"  FETCH_SFTP REPORT — {timestamp}")
print(f"  {settings_file}")
print("==================================================")

try:
    pkey     = load_private_key(dbutils.secrets.get(scope=settings["secret_scope"], key=settings["secret_key"]))
    host_key = load_host_key(settings["host_key"])
    # Download to local disk first, so a failed transfer never leaves a
    # partial file in the inbox for file_router to route.
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / destination.name
        # hostkey makes connect fail if the server's key doesn't match.
        with paramiko.Transport((host, port)) as transport:
            transport.connect(hostkey=host_key, username=settings["username"], pkey=pkey)
            with paramiko.SFTPClient.from_transport(transport) as sftp:
                sftp.get(remote_file, str(local))
        size = local.stat().st_size
        shutil.copyfile(local, destination)
except Exception as e:
    print("\n👎👎👎 Failed")
    print(f"   {host}:{remote_file}")
    print(f"   {type(e).__name__}: {e}")
    print("\n==================================================")
    raise

print("\n👍👍👍 Fetched")
print(f"   {host}:{remote_file} → {destination}")
print(f"   {size:,} bytes")
print("\n==================================================")
