# Databricks notebook source

import re
def get_latest_file_path(volume_path, date_pattern):
    """  
    This function takes two parameters as input and returns a  filename with max date as string 
    Args: 
        volume_path (string): This is the vol path where files are present. 
        date_pattern (string): This is the regular expression of date pattern in filename. 
    Returns: string: A filename with max date 
    """
    files = dbutils.fs.ls(volume_path)

    def extract_datetime(filename):
        match = re.search(date_pattern, filename)
        if match:
            return match.group(1)
        return None

    files_with_dates = [(f.path ,extract_datetime(f.name)) for f in files if extract_datetime(f.name) is not None]
    print(len(files_with_dates))
    latest_file = max(files_with_dates, key = lambda x: x[1], default = None)

    if latest_file:
        latest_file_path = latest_file[0].split("/")[-1]
        return latest_file_path
    else:
        print("no valid file found")

# COMMAND ----------

"""
The correct way they want us to do this is to use files instead of notebooks, but for the moment I'm going to do it this way. If you had a file and wanted to import its functions you would put "from file import *" in your code but that doesn't work with notebooks. Accessing the workspace through the sdk allows me to pull in functions from a notebook. During a clone process, notebooks are cloned, but files are not. I like %run better, but there is no way to use %run programmatically, such as loading any files that happen to exist in some subfolder at runtime, which is another thing I may end up doing.
"""
def import_notebook(path):
    import os
    import base64
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import workspace

    client = WorkspaceClient()

    try:
        if path.startswith("/"):
            response = client.workspace.export(path=path)
        else:
            response = client.workspace.export(path=f"{os.getcwd()}/{path}")
    except Exception as e:
        print("Caught RESOURCE_DOES_NOT_EXIST:", e)

    notebook_content = base64.b64decode(response.content).decode('utf-8')
    exec(notebook_content, globals())

# COMMAND ----------

import subprocess

"""
Runs a shell command and returns the output so it can be used.
"""
def getCmd(command, useShell=True):
    result = subprocess.run(command, shell=useShell, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return result.stdout.strip()

# COMMAND ----------

"""
Read a json file from the local filesystem and decode it into a variable.
"""
def read_json_and_decode(workspace_path):
    import json
    import os
    from pathlib import Path

    if isinstance(workspace_path, Path):
        resolved_path = workspace_path
    else:
        workspace_path = str(workspace_path)
        if "*" in workspace_path:
            matches = sorted(Path().glob(workspace_path))
            if len(matches) != 1:
                raise Exception("Expected exactly one file after interpreting globs in workspace path.")
            resolved_path = matches[0]
        elif workspace_path.startswith("/"):
            resolved_path = Path(workspace_path)
        else:
            resolved_path = Path(os.getcwd()) / workspace_path

    return json.loads(resolved_path.read_text(encoding="utf-8"))

# COMMAND ----------

"""
Encode a variable as json and save it as a plain json file.
"""
def encode_and_save_as_json(variable, workspace_path):
    import json
    import os
    from pathlib import Path

    resolved_path = Path(workspace_path)
    if not resolved_path.is_absolute():
        resolved_path = Path(os.getcwd()) / resolved_path

    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_path.write_text(json.dumps(variable, indent=4), encoding="utf-8")
