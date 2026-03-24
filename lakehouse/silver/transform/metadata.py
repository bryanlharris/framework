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
