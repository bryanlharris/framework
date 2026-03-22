from framework.core.utils import get_latest_file_path
from framework.transform.metadata import (
    add_source_metadata,
    add_timestamp_column,
)


def from_files(settings):
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
    df = (
        spark.readStream
        .format(source_type)
        .options(**readStreamOptions)
        .load(readStream_load)
    )

    df = add_timestamp_column(df, "ingest_time")
    df = add_source_metadata(df, "source_metadata")

    query_name = f"{catalog_name}_{bronze_schema}_{table}"
    query = (
        df.writeStream
        .format(writeStream_format)
        .outputMode(writeStream_outputMode)
        .queryName(query_name)
        .trigger(**trigger_type)
        .options(**writeStreamOptions)
        .toTable(dst_table_name)
    )
