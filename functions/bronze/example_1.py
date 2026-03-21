from functions.ingestFunctions import streaming_read, streaming_write
from functions.standardTransformations import (
    addRowShaChecksum,
    addSourceMetadata,
    addTimestampColumn,
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
    df = addTimestampColumn(df, "ingest_time")
    df = addSourceMetadata(df, "source_metadata")
    df = addRowShaChecksum(
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
