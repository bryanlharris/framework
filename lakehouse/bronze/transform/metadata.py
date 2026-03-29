def add_source_metadata(df, col_name):
    new_df = df.selectExpr("*", f"_metadata as {col_name}")
    return new_df


from pyspark.sql.functions import regexp_extract, to_timestamp, col

def add_derived_ingest_time(df, col_name, regex):
    """
    Extract a datetime string from source_metadata.file_path using regex,
    then parse it as a timestamp.

    The regex must contain a single capture group matching a string of the
    form YYYYMMDD_HHmmss, e.g. r'(\\d{8}_\\d{6})'.

    source_metadata must already be present on the DataFrame before this
    transform is called.
    """
    return df.withColumn(
        col_name,
        to_timestamp(
            regexp_extract(col("source_metadata.file_path"), regex, 1),
            "yyyyMMdd_HHmmss",
        ),
    )
