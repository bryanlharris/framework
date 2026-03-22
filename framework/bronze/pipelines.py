from pyspark.sql.functions import current_timestamp, expr

from framework.core.streaming import streaming_read, streaming_write
from framework.core.utils import get_latest_file_path, get_table_schema
from framework.transform.metadata import (
    add_row_sha_checksum,
    add_source_metadata,
    add_timestamp_column,
)


def example_1(settings):
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

    df = streaming_read(source_type, readStreamOptions, readStream_load)
    df = add_timestamp_column(df, "ingest_time")
    df = add_source_metadata(df, "source_metadata")
    df = add_row_sha_checksum(
        df,
        checksum_col_name="row_checksum",
        hash_cols="all",
        bitlength=256,
    )

    query_name = f"{catalog_name}_{bronze_schema}_{table}"
    streaming_write(
        df,
        dst_table_name,
        writeStream_format,
        writeStream_outputMode,
        query_name,
        trigger_type,
        writeStreamOptions,
    )


def example_2(settings):
    dst_table_name          = settings["dst_table_name"]
    catalog_name            = settings["dst_table_name"].split(".")[0]
    bronze_schema           = settings["dst_table_name"].split(".")[1]
    table                   = settings["dst_table_name"].split(".")[2]
    readStreamOptions       = settings["readStreamOptions"]
    writeStreamOptions      = settings["writeStreamOptions"]
    readStream_load         = settings["readStream_load"]
    writeStream_format      = settings["writeStream_format"]
    writeStream_outputMode  = settings["writeStream_outputMode"]

    (
        spark.readStream.format("cloudfiles")
        .options(**readStreamOptions)
        .load(readStream_load)
        .withColumn("ingest_time", current_timestamp())
        .withColumn("source_metadata", expr("_metadata"))
        .writeStream.format(writeStream_format)
        .options(**writeStreamOptions)
        .outputMode(writeStream_outputMode)
        .trigger(availableNow=True)
        .table(f"{catalog_name}.{bronze_schema}.{table}")
    )


def ingest_bronze(settings):
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
    trigger_type            = settings["trigger_type"]

    try:
        duplicatefiles_flag = settings["duplicatefiles_flag"]
        date_pattern = settings["date_pattern"]
        if duplicatefiles_flag is True:
            pathGlobFilter = get_latest_file_path(readStream_load, date_pattern)
            readStreamOptions["pathGlobFilter"] = pathGlobFilter
    except Exception:
        print("duplicatefiles_flag is false")

    if not bronze_schema.endswith("_raw"):
        raise Exception(
            """
                        Sanity checking failed.
                        Error: You are attempting to write to a non-raw schema with the bronze notebook.
                        The bronze notebook is only for writing to raw schemas.
                        Please examine your settings.
                        There is no parking in the red zone.
                        """
        )
    if (
        source_type.lower() == "cloudfiles"
        and "header" in readStreamOptions.keys()
        and readStreamOptions["header"] is False
    ):
        table_schema = get_table_schema(
            dst_table_name,
            ["source_metadata", "ingest_time", "row_checksum", "_rescued_data"],
        )
    else:
        table_schema = None

    df = streaming_read(
        source_type=source_type,
        readstream_options=readStreamOptions,
        source=readStream_load,
        schema=table_schema,
    )

    df = add_timestamp_column(df, "ingest_time")
    df = add_source_metadata(df, "source_metadata")

    query_name = f"{catalog_name}_{bronze_schema}_{table}"
    streaming_write(
        df,
        dst_table_name,
        writeStream_format,
        writeStream_outputMode,
        query_name,
        trigger_type,
        writeStreamOptions,
    )
