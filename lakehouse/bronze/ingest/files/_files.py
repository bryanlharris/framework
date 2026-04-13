

from lakehouse.core.transform.metadata import add_timestamp_column
from lakehouse.bronze.transform.file import add_source_metadata
from lakehouse.bronze.transform.derived import add_ingest_time_from_path

def from_url(spark, settings):
    """
    Download file from URL, write to landing volume, ingest via from_file.

    Required keys: download_url.
    Optional keys: filename (base name; timestamp is always appended before the extension).

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.files.from_url",
        "from_url_options": {
            "download_url": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDFUNDS",
            "filename": "fredgraph_FEDFUNDS.csv"
        }
    }
    """
    import requests
    from datetime import datetime
    from pathlib import Path

    opts = settings["from_url_options"]
    url = opts["download_url"]
    landing_path = settings["readStream_path"].rstrip("/")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_filename = url.split("/")[-1].split("?")[0]
    base = opts.get("filename", default_filename)
    stem = Path(base).stem
    suffix = Path(base).suffix
    filename = f"{stem}_{timestamp}{suffix}"

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

    Required keys: inbox_path, filename_pattern, landing_subdirectory.

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.files.from_inbox",
        "from_inbox_options": {
            "inbox_path": "/Volumes/edsm/bronze/inbox/",
            "filename_pattern": "systemsWithCoordinates7days.json",
            "landing_subdirectory": "/Volumes/edsm/bronze/landing/"
        }
    }
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


def from_pdf(spark, settings):
    """
    Read the latest PDF from a volume using binaryFile format, parse with
    ai_parse_document, and write to a Delta table in batch mode.

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.files.from_pdf",
        "input_path": "/Volumes/edsm/bronze/landing/",
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


def from_sftp(spark, settings):
    """
    Connect to an SFTP server using RSA key auth (from Databricks secrets), download
    files matching a pattern to the landing volume with timestamp-based naming, then
    ingest via from_file.

    Required: host, username, secret_scope, secret_key, remote_path.
    Optional: port (default 22), remote_filename_pattern (default "*").

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.files.from_sftp",
        "from_sftp_options": {
            "host": "sftp.example.com",
            "port": 22,
            "username": "svc_account",
            "secret_scope": "my-scope",
            "secret_key": "sftp-rsa-key",
            "remote_path": "/outbound/data/",
            "remote_filename_pattern": "export_*.csv"
        }
    }
    """
    import paramiko
    import fnmatch
    import io
    from datetime import datetime
    from pathlib import Path

    opts                    = settings["from_sftp_options"]
    host                    = opts["host"]
    port                    = opts.get("port", 22)
    username                = opts["username"]
    secret_scope            = opts["secret_scope"]
    secret_key              = opts["secret_key"]
    remote_path             = opts["remote_path"]
    remote_filename_pattern = opts.get("remote_filename_pattern", "*")
    landing_path            = Path(settings["readStream_path"].rstrip("/"))
    timestamp               = datetime.now().strftime("%Y%m%d_%H%M%S")

    private_key_string = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    pk = paramiko.RSAKey.from_private_key(io.StringIO(private_key_string))

    transport = paramiko.Transport((host, port))
    transport.connect(username=username, pkey=pk)
    sftp = paramiko.SFTPClient.from_transport(transport)

    for filename in sftp.listdir(remote_path):
        if fnmatch.fnmatch(filename, remote_filename_pattern):
            stem, suffix = Path(filename).stem, Path(filename).suffix
            dest = landing_path / f"{stem}_{timestamp}{suffix}"
            sftp.get(f"{remote_path.rstrip('/')}/{filename}", str(dest))

    sftp.close()
    transport.close()

    from_file(spark, settings)


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

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.files.from_file",
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

    df = (
        spark.readStream
        .format(source_type)
        .options(**readStream_options)
        .load(readStream_path)
        .transform(add_timestamp_column, "ingest_time")
        .transform(add_source_metadata, "source_metadata")
    )

    derived = settings.get("derived", {})
    derived_regex = derived.get("file_path_datetime_regex", r"(\d{8}_\d{6})")
    df = df.transform(add_ingest_time_from_path, "derived_ingest_time", derived_regex)

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
