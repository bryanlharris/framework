

from lakehouse.bronze.ingest.local import from_file


def from_url(spark, settings):
    """
    Download file from URL, write to landing volume, ingest via from_file.

    Required keys: download_url.
    Optional keys: filename (base name; timestamp is always appended before the extension).

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.net.from_url",
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


def from_sftp(spark, settings):
    """
    Connect to an SFTP server using RSA key auth (from Databricks secrets), download
    files matching a pattern to the landing volume with timestamp-based naming, then
    ingest via from_file.

    Required: host, username, secret_scope, secret_key, remote_path.
    Optional: port (default 22), remote_filename_pattern (default "*").

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.net.from_sftp",
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


def from_rest(spark, settings):
    """
    Call a REST API endpoint, write response as a JSON file to the landing
    volume, then ingest via from_file.

    Required keys: url.
    Optional keys: method (default GET), params (for GET), body (for POST), filename.

    Example settings (GET):
    {
        "function_path": "lakehouse.bronze.ingest.net.from_rest",
        "from_rest_options": {
            "url": "https://api.stlouisfed.org/fred/series/observations",
            "params": {
                "series_id": "FEDFUNDS",
                "api_key": "<YOUR_API_KEY>",
                "file_type": "json"
            }
        }
    }

    Example settings (POST):
    {
        "function_path": "lakehouse.bronze.ingest.net.from_rest",
        "from_rest_options": {
            "url": "https://api.osv.dev/v1/query",
            "method": "POST",
            "body": {
                "version": "2.1.3",
                "package": {
                    "name": "numpy",
                    "ecosystem": "PyPI"
                }
            },
            "filename": "osv_numpy"
        }
    }
    """
    import requests
    from datetime import datetime
    from pathlib import Path

    opts         = settings["from_rest_options"]
    url          = opts["url"]
    method       = opts.get("method", "GET").upper()
    params       = opts.get("params", {})
    body         = opts.get("body", {})
    landing_path = settings["readStream_path"].rstrip("/")
    timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem         = opts.get("filename", url.split("/")[-1])
    filename     = f"{stem}_{timestamp}.json"

    # Call API
    if method == "POST":
        response = requests.post(url, json=body, timeout=300)
    else:
        response = requests.get(url, params=params, timeout=300)
    response.raise_for_status()

    # Write JSON response to landing volume
    output_path = Path(landing_path) / filename
    output_path.write_bytes(response.content)

    # Execute standard file ingestion
    from_file(spark, settings)
