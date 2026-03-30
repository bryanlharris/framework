def add_source_metadata(df, col_name):
    """
    Add the Spark _metadata column (source file info) to the DataFrame under col_name.
    """
    new_df = df.selectExpr("*", f"_metadata as {col_name}")
    return new_df
