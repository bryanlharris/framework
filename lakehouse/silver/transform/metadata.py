from pyspark.sql.functions import sha2, concat_ws, coalesce, col, lit, current_timestamp


def add_row_sha_checksum(df, col_name='row_checksum', columns=None):
    if columns is not None:
        cols = [c.strip() for c in columns.split(",")] if isinstance(columns, str) else list(columns)
    else:
        cols = df.columns
    cols_expr = [coalesce(col(c).cast("string"), lit("-")) for c in cols]

    return df.withColumn(col_name, sha2(concat_ws('-', *cols_expr), 256))


def flatten_source_metadata(df):
    return df.select(
        "*",
        col("source_metadata.file_path").alias("file_path"),
        col("source_metadata.file_modification_time").alias(
            "file_modification_time"
        ),
        current_timestamp().alias("ingest_time"),
    )


def add_scd2_columns(df, ingest_time_column):
    from pyspark.sql.functions import col, lit
    return (
        df
        .withColumn("created_on", col(ingest_time_column))
        .withColumn("deleted_on", lit(None).cast("timestamp"))
        .withColumn("current_flag", lit("Yes"))
        .withColumn("valid_from", col(ingest_time_column))
        .withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp"))
    )
