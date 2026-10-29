from pyspark.sql.functions import current_timestamp


def add_timestamp_column(df, col_name):
    """
    Add a current_timestamp column to a DataFrame.
    """
    new_df = df.withColumn(col_name, current_timestamp())
    return new_df
