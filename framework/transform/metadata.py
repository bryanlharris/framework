from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import sha2, concat_ws, coalesce, col, lit


def add_row_sha_checksum(df, col_name='row_checksum', columns="all", separator='-', bitlength=256):
    if columns.lower() == "all":
        cols = df.columns
    else:
        cols = columns.split(",")

    cols_expr = [coalesce(col(c).cast("string"), lit("-")) for c in cols]

    return df.withColumn(col_name, sha2(concat_ws(separator, *cols_expr), bitlength))


def add_timestamp_column(df, col_name):
    new_df = df.withColumn(col_name, current_timestamp())
    return new_df


def add_source_metadata(df, col_name):
    new_df = df.selectExpr("*", f"_metadata as {col_name}")
    return new_df
