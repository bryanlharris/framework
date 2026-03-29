from lakehouse.silver.merge import scd2UpsertByBusinessKey
from lakehouse.silver.transform.metadata import add_row_sha_checksum, flatten_source_metadata, add_scd2_columns
from lakehouse.silver.transform.columns import cast_data_types, rename_columns


def scd2(spark, settings):
    source_table        = settings["source_table"]
    destination_table   = settings["destination_table"]
    column_map          = settings.get("column_map", {})
    data_type_map       = settings.get("data_type_map", {})
    writeStream_options = settings["writeStream_options"]
    business_key        = settings["business_key"]
    surrogate_key       = settings["surrogate_key"]
    readStream_options  = settings.get("readStream_options", {})
    use_row_hash        = settings.get("use_row_hash", False)
    row_hash_col        = settings.get("row_hash_col", "row_hash")
    ingest_time_column  = settings["ingest_time_column"]

    surrogate_key_str = ",".join(surrogate_key) if isinstance(surrogate_key, list) else surrogate_key

    df = (
        spark.readStream
        .options(**readStream_options)
        .table(source_table)
        .drop("ingest_time")
        .transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .transform(flatten_source_metadata)
        .transform(add_row_sha_checksum, col_name=row_hash_col, columns=surrogate_key_str)
        .transform(add_scd2_columns, ingest_time_column)
    )

    (
        df.writeStream
        .queryName(destination_table)
        .format("delta")
        .options(**writeStream_options)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(scd2UpsertByBusinessKey(business_key, surrogate_key, destination_table, ingest_time_column, use_row_hash, row_hash_col))
        .start()
    )
