# Databricks notebook source
"""
Build a dictionary based on a folder structure + json files.
"""
def build_dictionary(name):
    import os
    import json

    dictionary = {}
    json_files = getCmd(f"cd {name} && find . -type f -name '*.json'").split("\n")
    for path in json_files:
        path = path.removeprefix("./")
        if path:
            data = read_json_and_decode(f"{os.getcwd()}/{name}/{path}")
            parts = path.split("/")
            top = parts[0]
            if top.endswith(".json"):
                top = top.removesuffix(".json")
                current_level = dictionary
            else:
                current_level = dictionary.setdefault(top, {})
            for folder in parts[1:-1]:
                current_level = current_level.setdefault(folder, {})
            current_level[parts[-1].rsplit('.', 1)[0]] = data
    
    return dictionary

# COMMAND ----------

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

"""
Runs a multi-line string or list of SQL commands.
Each command needs to end with a semi-colon if it's string type.
"""
def run_sql_commands(sql_commands = None, dry_run = False):

    if sql_commands == None:
        raise Exception("""
                        Error: You ran the ``run_sql_commands'' function but did not supply any SQL commands.
                        The value of ``sql_commands'' was None or not supplied.
                        """)
    elif isinstance(sql_commands, str):
        sql_commands_list = sql_commands.split(';')
    elif isinstance(sql_commands, list):
        sql_commands_list = sql_commands 

    for sql_command in sql_commands_list:
        sql_command = sql_command.strip()
        if sql_command:
            print(sql_command)
            if not dry_run:
                spark.sql(sql_command)
    print()

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
Read a json file from the workspace and then decode it into a variable.
"""
def read_json_and_decode(workspace_path):
    import os
    import json
    import base64
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import workspace

    client = WorkspaceClient()

    # Allows workspace_path to be something like /Workspace/EDA/pipeline/settings/*task_settings/*datasync_settings.json
    if "*" in workspace_path:
        interpret_globs = getCmd(f"dir -1 {workspace_path}")
        if "\n" in interpret_globs:
            raise Exception("Found more than one file after interpreting globs in workspace path.")
        else:
            workspace_path = interpret_globs

    # Allows workspace_path to optionally start with a "/" character
    try:
        if workspace_path.startswith("/"):
            response = client.workspace.export(path=workspace_path, format=workspace.ExportFormat.SOURCE)
        else:
            response = client.workspace.export(path=f"{os.getcwd()}/{workspace_path}", format=workspace.ExportFormat.SOURCE)
    except Exception as e:
        print("Caught RESOURCE_DOES_NOT_EXIST:", e)

    variable = base64.b64decode(response.content).decode('utf-8')
    variable = variable.replace("# Databricks notebook source\n", "")
    variable = json.loads(variable)

    return variable

# COMMAND ----------

"""
Encode a variable as json and save it as a json file in the workspace.
"""
def encode_and_save_as_json(variable, workspace_path):
    import json
    import base64
    from pathlib import Path
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import workspace

    json_string = json.dumps(variable, indent=4)
    encoded_content = base64.b64encode(json_string.encode()).decode()
    path = str(Path(workspace_path).parent)

    client = WorkspaceClient()
    client.workspace.mkdirs(path)

    client.workspace.import_(
        content=encoded_content,
        format=workspace.ImportFormat.AUTO,
        overwrite=True,
        path=workspace_path
    )