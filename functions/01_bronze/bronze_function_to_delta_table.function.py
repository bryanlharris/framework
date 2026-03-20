# Databricks notebook source
import json
from textwrap import dedent
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import when, col, to_timestamp, to_date, lit
from pyspark.sql.types import StringType, StructType
from functions.transform_registry import TRANSFORM_REGISTRY

# COMMAND ----------

def bronze_function_to_delta_table(settings):

    # Variables (workflow)
    pipeline = dbutils.widgets.get("pipeline")
    task_settings = json.loads(dbutils.widgets.get("task_settings"))
    full_table_name = task_settings['full_table_name']

    # Variables (json file)
    dst_table_name          = settings["dst_table_name"]
    catalog_name            = settings["dst_table_name"].split(".")[0]
    bronze_schema           = settings["dst_table_name"].split(".")[1]
    table                   = settings["dst_table_name"].split(".")[2]
    readStreamOptions       = settings["readStreamOptions"]
    writeStreamOptions      = settings["writeStreamOptions"]
    readStream_load         = settings["readStream_load"]
    writeStream_format      = settings["writeStream_format"]
    writeStream_outputMode  = settings["writeStream_outputMode"]
    source_type             = settings["source_type"]
    transform_functions     = settings["transform_functions"]
    trigger_type            = settings["trigger_type"]

    try:
        duplicatefiles_flag = settings["duplicatefiles_flag"]
        date_pattern        = settings["date_pattern"]
        if duplicatefiles_flag == True:
            pathGlobFilter = get_latest_file_path(readStream_load, date_pattern)
            readStreamOptions["pathGlobFilter"] = pathGlobFilter
    except:
        print("duplicatefiles_flag is false")

    # Sanity check
    if not bronze_schema.endswith("_raw"):
        raise Exception("""
                        Sanity checking failed.
                        Error: You are attempting to write to a non-raw schema with the bronze notebook.
                        The bronze notebook is only for writing to raw schemas.
                        Please examine your settings.
                        There is no parking in the red zone.
                        """)
    if source_type.lower() == "cloudfiles" and "header" in readStreamOptions.keys() and readStreamOptions["header"] == False:
        table_schema = get_table_schema(dst_table_name, ["source_metadata","ingest_time", "row_checksum", "_rescued_data"])
    else:
        table_schema = None
        
    df_data = streaming_read(source_type = source_type, 
        readstream_options = readStreamOptions, source = readStream_load, schema = table_schema)

    # get and apply the transformation functions from config table
    if transform_functions != None:
        for function_name, parameters in transform_functions.items():
            func = TRANSFORM_REGISTRY.get(function_name)

            if func is None:
                fallback_func = globals().get(function_name)
                if fallback_func is not None:
                    func = fallback_func
                else:
                    available_transforms = ", ".join(sorted(TRANSFORM_REGISTRY.keys()))
                    raise KeyError(
                        f"Transform '{function_name}' was not found in TRANSFORM_REGISTRY or globals(). "
                        f"Available registry transforms: {available_transforms}"
                    )

            df_data = applyTransformFunction(df_data, func, parameters)

    # if duplicatefiles_flag == True:
    #     df_data = pick_latest_file_from_volpath(df_data, date_pattern)

    # write data to target table
    query_name = f"{catalog_name}_{schema_name}_{table}"
    streaming_write(
                df_data,
                dst_table_name, 
                writeStream_format, 
                writeStream_outputMode, 
                query_name, 
                trigger_type, 
                writeStreamOptions
            )