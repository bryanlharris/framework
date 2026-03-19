# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook initiates a datasync task. It takes paramemters **task_arn**, **bytes_per_second**, **skip**, and **stop_here**.
# MAGIC
# MAGIC **task_arn** is set via `./settings/task_settings/datasync_settings.json`. Remaining parameters are set via the Workflow GUI.
# MAGIC
# MAGIC If you are doing a first-time run you might want to increase **bytes_per_second**. For later runs you might want to set **skip*** to True.

# COMMAND ----------

# MAGIC %run ./functions/utility

# COMMAND ----------

import json

# Workflow parameters and task settings
pipeline            = dbutils.widgets.get("pipeline")
skip                = dbutils.widgets.get("skip")
stop_here           = dbutils.widgets.get("stop_here")
bytes_per_second    = dbutils.widgets.get("bytes_per_second")
task_settings       = json.loads(dbutils.widgets.get("task_settings"))
task_arn            = task_settings['task_arn']

# Skip if True (continues remaining workflow)
if skip == "True":
    dbutils.notebook.exit("Skipping per Workflow task settings.")

# Stop if True (stops and does not continue workflow)
if stop_here == "True":
    raise Exception("Stop here per task settings.")

# COMMAND ----------

import json
import boto3
import time
import pprint
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from botocore.config import Config

# Bytes per second
bytes_per_second = eval(bytes_per_second)

# Set the Datasync config
config = Config(
    region_name="us-east-1",
    retries={
        "max_attempts": 2,
        "mode": "standard"
    }
)

# Get the access and secret keys from the Databricks secret scope
aws_access_key_id = dbutils.secrets.get(scope="datasync_scope", key="aws_access_key_id")
aws_secret_access_key = dbutils.secrets.get(scope="datasync_scope", key="aws_secret_access_key")

# Setup the task options
datasync_client = boto3.client(
    "datasync",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    config=config
)

# Build a dictionary of tags
tags = datasync_client.list_tags_for_resource(
    ResourceArn=task_arn,
    MaxResults=100
).get("Tags", [])

# Filter out certain tags
tags = [tag for tag in tags if "cfpb:" in tag["Key"]]
tags = [tag for tag in tags if not tag["Key"].startswith("aws:")]

# This runs the task and gets an identifier we can use later
response = datasync_client.start_task_execution(
    TaskArn=task_arn,
    OverrideOptions={
        "OverwriteMode": "ALWAYS",
        'PreserveDeletedFiles':"PRESERVE",
        'TaskQueueing': 'ENABLED',
        'BytesPerSecond': bytes_per_second
        },
    Tags=tags
    )
task_execution_arn=response['TaskExecutionArn']

# This waits on the task to get finished.
while True:
    task_execution = datasync_client.describe_task_execution(TaskExecutionArn=task_execution_arn)
    status = task_execution['Status']
    if status in ['SUCCESS', 'ERROR']:
        break
    time.sleep(5)

if status == 'SUCCESS':
    print(f"""
          Datasync task succeeded.
          See below or AWS Cloudwatch logs for more info.

          {pprint.pprint(task_execution)}
          """)
else:
    # Raising an exception will stop the outer workflow.
    raise Exception(f"""
                    Datasync task failed.
                    See below or AWS Cloudwatch logs for more info.

                    {pprint.pprint(task_execution)}
                    {print(json.dumps(task_execution, indent=4))}
                    """)