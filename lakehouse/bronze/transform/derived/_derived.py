from pyspark.sql.functions import regexp_extract, to_timestamp, col

def add_ingest_time_from_path(df, col_name, regex):
    """
    Extract a datetime string from source_metadata.file_path using regex,
    then parse it as a timestamp.

    The regex must contain exactly one capture group that matches a datetime
    string in the format YYYYMMDD_HHmmss (e.g. r'(\\d{8}_\\d{6})').
    The captured value is parsed with the fixed format "yyyyMMdd_HHmmss".

    The regex itself can vary to handle different path structures — for example,
    anchoring to a specific directory or filename prefix — but the captured
    group must always produce a string in that exact format.

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
