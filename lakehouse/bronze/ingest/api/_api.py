

from lakehouse.core.transform.metadata import add_timestamp_column
from lakehouse.bronze.transform.file import add_source_metadata


def from_rest_api(spark, settings):
    """
    Call a REST API endpoint, write response as a JSON file to the landing
    volume, then ingest via from_file.

    Reads function-specific options from settings["from_rest_api_options"].
    Required keys: url, params.
    """
    import requests
    from datetime import datetime
    from pathlib import Path
    from lakehouse.bronze.ingest.files import from_file

    opts         = settings["from_rest_api_options"]
    url          = opts["url"]
    params       = opts["params"]
    landing_path = settings["readStream_path"].rstrip("/")
    timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem         = url.split("/")[-1]
    filename     = f"{stem}_{timestamp}.json"

    # Call API
    response = requests.get(url, params=params, timeout=300)
    response.raise_for_status()

    # Write JSON response to landing volume
    output_path = Path(landing_path) / filename
    output_path.write_bytes(response.content)

    # Execute standard file ingestion
    from_file(spark, settings)
