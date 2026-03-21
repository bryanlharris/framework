from functions.commonFunctions import applyTransformFunction, get_table_schema
from functions.ingestFunctions import streaming_read, streaming_write
from functions.transforms import TRANSFORM_REGISTRY
from functions.utility import get_latest_file_path


def ingest_bronze(settings):
    dst_table_name = settings["dst_table_name"]
    catalog_name = settings["dst_table_name"].split(".")[0]
    bronze_schema = settings["dst_table_name"].split(".")[1]
    table = settings["dst_table_name"].split(".")[2]
    readStreamOptions = settings["readStreamOptions"]
    writeStreamOptions = settings["writeStreamOptions"]
    readStream_load = settings["readStream_load"]
    writeStream_format = settings["writeStream_format"]
    writeStream_outputMode = settings["writeStream_outputMode"]
    source_type = settings["source_type"]
    transform_functions = settings["transform_functions"]
    trigger_type = settings["trigger_type"]

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
    if (
        source_type.lower() == "cloudfiles"
        and "header" in readStreamOptions.keys()
        and readStreamOptions["header"] is False
    ):
        table_schema = get_table_schema(
            dst_table_name,
            ["source_metadata", "ingest_time", "row_checksum", "_rescued_data"],
        )
    else:
        table_schema = None

    df = streaming_read(
        source_type=source_type,
        readstream_options=readStreamOptions,
        source=readStream_load,
        schema=table_schema,
    )

    if transform_functions is not None:
        for function_name, parameters in transform_functions.items():
            func = TRANSFORM_REGISTRY.get(function_name)

            if func is None:
                available_transforms = ", ".join(sorted(TRANSFORM_REGISTRY.keys()))
                raise KeyError(
                    f"Transform '{function_name}' was not found in TRANSFORM_REGISTRY. "
                    f"Available transforms: {available_transforms}"
                )

            df = df.transform(func, *parameters)

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
