

from lakehouse.core.transform.metadata import add_timestamp_column
from lakehouse.bronze.transform.metadata import add_source_metadata

def from_url(spark, settings):
    """
    Download file from URL, write to landing volume, ingest via from_file.

    Reads function-specific options from settings["from_url_options"].
    Required keys: download_url.
    Optional keys: filename (defaults to URL basename with timestamp).
    """
    import requests
    from datetime import datetime
    from pathlib import Path

    opts = settings["from_url_options"]
    url = opts["download_url"]
    landing_path = settings["readStream_path"].rstrip("/")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_filename = url.split("/")[-1]
    stem = Path(default_filename).stem
    suffix = Path(default_filename).suffix
    filename = opts.get("filename", f"{stem}_{timestamp}{suffix}")

    # Download
    response = requests.get(url, timeout=300)
    response.raise_for_status()

    # Write to landing volume
    output_path = Path(landing_path) / filename
    output_path.write_bytes(response.content)

    # Execute standard file ingestion
    from_file(spark, settings)


def from_inbox(spark, settings):
    """Move files from inbox volume to landing zone with timestamp-based naming, then ingest.

    Reads function-specific options from settings["from_inbox_options"].
    Required keys: inbox_path, filename_pattern, landing_subdirectory.
    """
    import shutil
    from datetime import datetime
    from pathlib import Path

    opts     = settings["from_inbox_options"]
    inbox    = Path(opts["inbox_path"])
    pattern  = opts["filename_pattern"]
    landing  = Path(opts["landing_subdirectory"])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for file in inbox.glob(pattern):
        stem   = Path(file.name).stem
        suffix = Path(file.name).suffix
        new_name = f"{stem}_{timestamp}{suffix}"
        dest = landing / new_name
        shutil.move(str(file), str(dest))

    # Execute standard file ingestion
    from_file(spark, settings)


def from_file(spark, settings):
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

    df = (
        spark.readStream
        .format(source_type)
        .options(**readStream_options)
        .load(readStream_path)
        .transform(add_timestamp_column, "ingest_time")
        .transform(add_source_metadata, "source_metadata")
    )

    query_name = destination_table
    query = (
        df.writeStream
        .format(writeStream_format)
        .outputMode(writeStream_outputMode)
        .queryName(query_name)
        .trigger(**trigger_type)
        .options(**writeStream_options)
        .toTable(destination_table)
    )
