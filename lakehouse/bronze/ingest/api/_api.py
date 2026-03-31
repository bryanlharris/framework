

from lakehouse.core.transform.metadata import add_timestamp_column
from lakehouse.bronze.transform.file import add_source_metadata


def from_rest_api(spark, settings):
    """
    Call a REST API endpoint, write response as a JSON file to the landing
    volume, then ingest via from_file.

    Reads function-specific options from settings["from_rest_api_options"].
    Required keys: url, params.

    Example settings:
    {
        "function_path": "lakehouse.bronze.ingest.api.from_rest_api",
        "from_rest_api_options": {
            "url": "https://api.stlouisfed.org/fred/series/observations",
            "params": {
                "series_id": "FEDFUNDS",
                "api_key": "<YOUR_API_KEY>",
                "file_type": "json"
            }
        },
        "source_type": "cloudFiles",
        "destination_table": "demo.bronze.fedfunds",
        "readStream_path": "/Volumes/demo/bronze/landing/",
        "readStream_options": {
            "cloudFiles.format": "json",
            "cloudFiles.inferColumnTypes": "false",
            "cloudFiles.inferSchema": "true",
            "cloudFiles.schemaLocation": "/Volumes/demo/bronze/utility/demo.bronze.fedfunds/_schema/",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "cloudFiles.useNotifications": "false",
            "cloudFiles.useIncrementalListing": "auto",
            "cloudFiles.validateOptions": "true",
            "badRecordsPath": "/Volumes/demo/bronze/utility/demo.bronze.fedfunds/_badRecords/",
            "multiLine": "false",
            "columnNameOfCorruptRecord": "corrupt_record",
            "pathGlobFilter": "observations*.json"
        },
        "writeStream_format": "delta",
        "writeStream_options": {
            "mergeSchema": "true",
            "checkpointLocation": "/Volumes/demo/bronze/utility/demo.bronze.fedfunds/_checkpoints/",
            "delta.columnMapping.mode": "name"
        },
        "writeStream_outputMode": "append",
        "trigger_type": { "availableNow": true }
    }
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
