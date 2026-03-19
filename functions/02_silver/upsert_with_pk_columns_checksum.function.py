# Databricks notebook source
def upsert_with_pk_columns_checksum(settings):
    from pyspark.sql.functions import col, current_timestamp

    # Variables (json file)
    src_table_name          = settings["src_table_name"]
    dst_table_name          = settings["dst_table_name"]
    fuzzy_column_map        = settings["fuzzy_column_map"]
    data_type_map           = settings["data_type_map"]
    catalog_name            = settings["dst_table_name"].split(".")[0]
    silver_schema           = settings["dst_table_name"].split(".")[1]
    table                   = settings["dst_table_name"].split(".")[2]
    readStreamOptions       = settings["readStreamOptions"]
    writeStreamOptions      = settings["writeStreamOptions"]
    writeStream_outputMode  = settings["writeStream_outputMode"]
    pk                      = settings["pk"]["name"]
    pk_columns              = settings["pk"]["columns"]
    source_type             = settings["source_type"]

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    (
        streaming_read(source_type, **readStreamOptions, src_table_name)
        .drop("ingest_time")
        .rename_columns(column_map)
        .cast_data_types(data_type_map)
        .select(
            "*",
            col("source_metadata.file_path").alias("file_path"),
            col("source_metadata.file_modification_time").alias("file_modification_time"),
            current_timestamp().alias("ingest_time")
        )
        .addRowShaChecksumWithSep(checksum_col_name=pk, hash_cols=pk_columns, seperator='', bitlength=256 )
        .writeStream
        .queryName(dst_table_name)
        .format("delta")
        .options(**writeStreamOptions)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(upsertToDelta(pk, dst_table_name, pk))
        .start()
    )