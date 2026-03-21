from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import sha2, concat_ws, coalesce, col, lit


def addRowShaChecksum(df, checksum_col_name='row_checksum', hash_cols="all", separator='-', bitlength=256):
    if hash_cols.lower() == "all":
        cols = df.columns
    else:
        cols = hash_cols.split(",")

    cols_expr = [coalesce(col(c).cast("string"), lit("-")) for c in cols]

    return df.withColumn(checksum_col_name, sha2(concat_ws(separator, *cols_expr), bitlength))


def addTimestampColumn(df, colName):
    new_df = df.withColumn(colName, current_timestamp())
    return new_df


def addSourceMetadata(df, colName):
    new_df = df.selectExpr("*", f"_metadata as {colName}")
    return new_df
