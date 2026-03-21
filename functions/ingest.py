from functions.commonFunctions import applyTransformFunction, get_table_schema
from functions.transforms import transform_registry
from functions.utility import get_latest_file_path
from delta.tables import DeltaTable


def ingest_bronze(settings):
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
    transform_functions     = settings["transform_functions"]
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
            func = transform_registry.get(function_name)

            if func is None:
                available_transforms = ", ".join(sorted(transform_registry.keys()))
                raise KeyError(
                    f"Transform '{function_name}' was not found in transform_registry. "
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







def streaming_read(source_type, readstream_options, source, schema = None):
    if source_type.lower() == "cloudfiles" and schema:
        df = (
            spark.readStream
            .schema(schema)
            .format(source_type)
            .options(**readstream_options)
            .load(source)
        )
    elif source_type.lower() == "cloudfiles" and not schema:
        df = (
            spark.readStream
            .format(source_type)
            .options(**readstream_options)
            .load(source)
        )
    elif source_type.lower() == "table" and readstream_options:
        df = (
            spark.readStream
            .options(**readstream_options)
            .table(source)
        )
    elif source_type.lower() == "table" and not readstream_options:
        df = (
            spark.readStream
            .table(source)
        )
    return df


def streaming_write(df, target_table, table_format, output_mode, query_name, trigger_type, writestream_options):
    query = (
        df.writeStream
        .format(table_format)
        .outputMode(output_mode)
        .queryName(query_name)
        .trigger(**trigger_type)
        .options(**writestream_options)
        .toTable(target_table)
    )
    return query


def truncateAndUpsertToDeltaWithKeysSQL(mergeKeys, destinationTable):
    def _do_upsert(microBatchDF, batchId):
        merge_condition = " AND ".join([f"source.{col} = target.{col}" for col in mergeKeys])

        microBatchDF.createOrReplaceTempView("microBatchDF")

        microBatchDF.sparkSession.sql(f"""
            MERGE INTO {destinationTable} AS target
            USING microBatchDF AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)

    return _do_upsert


def truncateAndUpsertToDeltaWithKeys(mergeKeys, destinationTable):
    def _do_upsert(microBatchDF, batchId):
        deltaTable = DeltaTable.forName(spark, destinationTable)

        merge_condition = " AND ".join([f"s.{col} = d.{col}" for col in mergeKeys])
        (
            deltaTable.alias("d")
            .merge(microBatchDF.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
    return _do_upsert


def upsertToDeltaWithPK(sourcePK, destinationTable, destinationPK):
    def _do_upsert(microBatchDF, batchId):
        deltaTable = DeltaTable.forName(spark, destinationTable)
        (
            deltaTable.alias("d")
            .merge(microBatchDF.alias("s"), f"s.{sourcePK} = d.{destinationPK}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    return _do_upsert
