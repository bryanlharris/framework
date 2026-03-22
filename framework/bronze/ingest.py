from framework.transform.metadata import (
    add_source_metadata,
    add_timestamp_column,
)


def from_files(settings):
    destination_table       = settings["destination_table"]
    catalog_name            = settings["destination_table"].split(".")[0]
    bronze_schema           = settings["destination_table"].split(".")[1]
    table                   = settings["destination_table"].split(".")[2]
    readStream_options      = settings["readStream_options"]
    writeStream_options     = settings["writeStream_options"]
    readStream_path         = settings["readStream_path"]
    writeStream_format      = settings["writeStream_format"]
    writeStream_outputMode  = settings["writeStream_outputMode"]
    source_type             = settings["source_type"]
    trigger_type            = settings["trigger_type"]

    if bronze_schema != "bronze":
        raise Exception(
            """
            Sanity checking failed.
            Error: You are attempting to write to a non-bronze schema with the bronze notebook.
            The bronze notebook is only for writing to the bronze schema.
            Please examine your settings.
            There is no parking in the red zone.
            """
        )

    df = (
        spark.readStream
        .format(source_type)
        .options(**readStream_options)
        .load(readStream_path)
        .transform(add_timestamp_column, "ingest_time")
        .transform(add_source_metadata, "source_metadata")
    )

    query_name = f"{catalog_name}_{bronze_schema}_{table}"
    query = (
        df.writeStream
        .format(writeStream_format)
        .outputMode(writeStream_outputMode)
        .queryName(query_name)
        .trigger(**trigger_type)
        .options(**writeStream_options)
        .toTable(destination_table)
    )
