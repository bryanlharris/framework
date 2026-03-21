from pyspark.sql.functions import col, current_timestamp

from functions.ingestFunctions import streaming_read, upsertToDeltaWithPK
from functions.standardTransformations import (
    addRowShaChecksum,
    cast_data_types,
    rename_columns,
)


def upsert_with_pk_columns_checksum(settings):
    src_table_name      = settings["src_table_name"]
    dst_table_name      = settings["dst_table_name"]
    fuzzy_column_map    = settings["fuzzy_column_map"]
    data_type_map       = settings["data_type_map"]
    writeStreamOptions  = settings["writeStreamOptions"]
    pk                  = settings["pk"]["name"]
    pk_columns          = settings["pk"]["columns"]
    pk_columns_str      = ",".join(pk_columns) if isinstance(pk_columns, list) else pk_columns
    source_type         = settings["source_type"]
    readStreamOptions   = settings["readStreamOptions"]

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    (
        streaming_read(source_type, readStreamOptions, src_table_name)
        .drop("ingest_time")
        .transform(rename_columns, fuzzy_column_map)
        .transform(cast_data_types, data_type_map)
        .select(
            "*",
            col("source_metadata.file_path").alias("file_path"),
            col("source_metadata.file_modification_time").alias(
                "file_modification_time"
            ),
            current_timestamp().alias("ingest_time"),
        )
        .transform(
            addRowShaChecksum,
            checksum_col_name=pk,
            hash_cols=pk_columns_str,
            seperator="",
            bitlength=256,
        )
        .writeStream
        .queryName(dst_table_name)
        .format("delta")
        .options(**writeStreamOptions)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(upsertToDeltaWithPK(pk, dst_table_name, pk))
        .start()
    )
