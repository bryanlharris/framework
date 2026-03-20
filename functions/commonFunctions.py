

def get_table_schema(table_name, columns_to_drop = []):
    df = spark.read.table(table_name)
    if len(columns_to_drop) > 0:
        schema = df.drop(*columns_to_drop).schema
    else:
        schema = df.schema
    return schema


def applyTransformFunction(df, function_name, parameters):
    transformed_df = df.transform(function_name, *parameters)
    return transformed_df
