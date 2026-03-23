

from lakehouse.transform.metadata import (
    add_source_metadata,
    add_timestamp_column,
)

def from_url(settings):
    """Download file from URL, decompress if gzipped, write to landing volume, ingest via from_files.

    Additional required settings:
        download_url: URL to download from (str)
        filename: Optional output filename (defaults to URL basename with .gz removed)
    """
    import requests
    import gzip
    from datetime import datetime
    from pathlib import Path

    url = settings["download_url"]
    landing_path = settings["readStream_path"].rstrip("/")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_filename = url.split("/")[-1].replace(".gz", "")
    stem = Path(default_filename).stem
    suffix = Path(default_filename).suffix
    filename = settings.get("filename", f"{stem}_{timestamp}{suffix}")

    # Download
    response = requests.get(url, timeout=300)
    response.raise_for_status()

    # Decompress if gzipped
    content = gzip.decompress(response.content) if url.endswith(".gz") else response.content

    # Write to landing volume
    output_path = Path(landing_path) / filename
    output_path.write_bytes(content)

    # Execute standard file ingestion
    from_files(settings)


def from_files(settings):
    destination_table       = settings["destination_table"]
    readStream_options      = settings["readStream_options"]
    writeStream_options     = settings["writeStream_options"]
    readStream_path         = settings["readStream_path"]
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
