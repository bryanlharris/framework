from pyspark.sql.functions import current_timestamp


def add_timestamp_column(df, col_name):
    new_df = df.withColumn(col_name, current_timestamp())
    return new_df
