# Databricks notebook source
def example_1(settings):
    import json

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
    trigger_type            = settings["trigger_type"]
    source_type             = settings["source_type"]

    # Read, transform, write
    (
        streaming_read(source_type, readStreamOptions, readStream_load)
        .addTimestampColumn("ingest_time")
        .addSourceMetadata("source_metadata")
        .addRowShaChecksum(checksum_col_name = 'row_checksum', hash_cols = "all", bitlength=256)
        .streaming_write(dst_table_name, writeStream_format, writeStream_outputMode, table, trigger_type, writeStreamOptions)
    )