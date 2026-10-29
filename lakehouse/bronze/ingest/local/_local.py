

from lakehouse.core.transform.metadata import add_timestamp_column
from lakehouse.bronze.transform.file import add_source_metadata
from lakehouse.bronze.transform.derived import add_ingest_time_from_path


def from_pdf(spark, settings):
    """
    Read the latest PDF from a volume using binaryFile format, parse with
    ai_parse_document, and write to a Delta table in batch mode.

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.local.from_pdf",
        "input_path": "/Volumes/utility/raw/files/edsm/",
        "date_pattern": "(\\d{8}_\\d{6})"
    }
    """
    from lakehouse.core.transform.metadata import add_timestamp_column
    from lakehouse.core.utils import ensure_table_exists
    from lakehouse.bronze.utils import get_latest_file_path

    destination_table  = settings["destination_table"]
    input_path         = settings["input_path"]
    date_pattern       = settings["date_pattern"]
    write_options      = settings["write_options"]
    write_mode         = settings.get("write_mode", "append")

    latest_file        = get_latest_file_path(input_path, date_pattern)
    full_path          = f"{input_path.rstrip('/')}/{latest_file}"

    df = (
        spark.read
        .format("binaryFile")
        .load(full_path)
        .selectExpr(
            "path",
            "modificationTime as source_modified_time",
            "ai_parse_document(content) AS parsed_document"
        )
        .transform(add_timestamp_column, "ingest_time")
    )

    schema_string = ", ".join(f"`{name}` {dtype}" for name, dtype in df.dtypes)
    ensure_table_exists(spark, destination_table, schema_string)

    (
        df.write
        .format("delta")
        .options(**write_options)
        .mode(write_mode)
        .saveAsTable(destination_table)
    )


def from_file(spark, settings):
    """
    Read a streaming file source, add ingest_time and source_metadata columns,
    derive ingest time from the file path, and write to a bronze Delta table.
    The destination_table must be in the bronze schema.

    Contract: every table produced by this function will contain the following columns.
    These names are fixed and referenced by downstream silver transforms — do not rename them.
      - ingest_time: current timestamp at the time of ingestion
      - source_metadata: struct containing file_path and file_modification_time
      - derived_ingest_time: timestamp parsed from the file path using file_path_datetime_regex
                             and file_path_datetime_format (default "yyyyMMdd_HHmmss")

    dedupe_within_file (optional, default false): drop rows repeated inside one
    file before the append. Repeats across files are kept.

    Recovery options (set in readStream_options):
      - modifiedAfter: skip files with modification time at or before this
                       ISO 8601 timestamp, e.g. "2026-04-10T17:10:35.000Z".
                       Remove after a successful recovery run.

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.local.from_file",
        "derived": {
            "file_path_datetime_regex": "(\\d{8}_\\d{6})\\.json"
        }
    }
    """
    destination_table       = settings["destination_table"]
    readStream_options      = settings["readStream_options"]
    writeStream_options     = settings["writeStream_options"]
    readStream_path         = settings["readStream_path"]

    if "badRecordsPath" in readStream_options and "mode" in readStream_options:
        raise ValueError(
            "If 'badRecordsPath' is specified, 'mode' is not allowed to be set. "
            "Remove 'mode' from readStream_options — badRecordsPath implicitly uses PERMISSIVE mode."
        )
    writeStream_format      = settings["writeStream_format"]
    writeStream_outputMode  = settings["writeStream_outputMode"]
    source_type             = settings["source_type"]
    trigger_type            = settings["trigger_type"]

    if destination_table.split(".")[1] != "bronze":
        raise Exception(
            """
            Sanity checking failed.
            Error: You are attempting to write to a non-bronze schema with the bronze notebook.
            The bronze notebook is only for writing to the bronze schema.
            Please examine your settings.
            There is no parking in the red zone.
            """
        )

    derived = settings.get("derived", {})
    derived_regex = derived.get("file_path_datetime_regex", r"(\d{8}_\d{6})")
    derived_format = derived.get("file_path_datetime_format", "yyyyMMdd_HHmmss")
    df = (
        spark.readStream
        .format(source_type)
        .options(**readStream_options)
        .load(readStream_path)
        .transform(add_timestamp_column, "ingest_time")
        .transform(add_source_metadata, "source_metadata")
        .transform(add_ingest_time_from_path, "derived_ingest_time", derived_regex, derived_format)
    )

    query_name = destination_table
    writer = (
        df.writeStream
        .format(writeStream_format)
        .outputMode(writeStream_outputMode)
        .queryName(query_name)
        .trigger(**trigger_type)
        .options(**writeStream_options)
    )

    if settings.get("dedupe_within_file", False):
        # Drop rows repeated inside one file. Each batch reads a file once and
        # every row carries its file_path, so repeats across files are kept.
        def _append_deduped(microBatchDF, batchId):
            (
                microBatchDF.dropDuplicates()
                .write
                .format(writeStream_format)
                .options(**writeStream_options)
                .option("txnAppId", query_name)
                .option("txnVersion", batchId)
                .mode("append")
                .saveAsTable(destination_table)
            )
        query = writer.foreachBatch(_append_deduped).start()
    else:
        query = writer.toTable(destination_table)
