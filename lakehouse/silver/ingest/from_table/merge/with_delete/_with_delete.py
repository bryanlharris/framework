from lakehouse.silver.merge import fullSyncMergeSQL
from lakehouse.silver.transform.metadata import add_row_sha_checksum, flatten_source_metadata
from lakehouse.silver.transform.columns import cast_data_types, rename_columns


def with_delete(spark, settings):
    """
    Read streaming data from a bronze table, apply column renames, type casts,
    metadata flattening, and row checksums, then perform a full-sync merge into a
    silver Delta table via SQL MERGE, handling INSERT, UPDATE, and DELETE.

    Example settings:
    {
        "function_path": "lakehouse.silver.ingest.from_table.merge.with_delete",
        "column_map": {
            "id64": "system_id"
        },
        "data_type_map": {
            "system_id": "long",
            "date": "timestamp"
        },
        "pk": {
            "name": "primary_key",
            "columns": ["system_id"]
        }
    }
    """
    source_table        = settings["source_table"]
    destination_table   = settings["destination_table"]
    column_map          = settings["column_map"]
    data_type_map       = settings["data_type_map"]
    writeStream_options = settings["writeStream_options"]
    pk                  = settings["pk"]["name"]
    pk_columns          = settings["pk"]["columns"]
    pk_columns_str      = ",".join(pk_columns) if isinstance(pk_columns, list) else pk_columns
    readStream_options  = settings["readStream_options"]

    df = (
        spark.readStream
        .options(**readStream_options)
        .table(source_table)
        .drop("ingest_time")
        .transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .transform(flatten_source_metadata)
        .transform(add_row_sha_checksum, col_name=pk, columns=pk_columns_str)
    )

    (
        df.writeStream
        .queryName(destination_table)
        .format("delta")
        .options(**writeStream_options)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(fullSyncMergeSQL(pk_columns, destination_table))
        .start()
    )
