def add_source_metadata(df, col_name):
    new_df = df.selectExpr("*", f"_metadata as {col_name}")
    return new_df
