# Databricks notebook source

def get_table_schema(table_name, columns_to_drop = []):
    """
    Retrieves the schema of a given table in a Spark DataFrame and optionally drops specific columns from the schema. 
    
    Parameters:
    ---------- 
        table_name : str 
            The name of the table to retrieve the schema from. 
        columns_to_drop : list, optional 
            A list of column names to drop from the table schema. Default is an empty list, which means no columns will be dropped. 
    Returns: 
    ------- 
    pyspark.sql.types.StructType 
        The schema of the table after dropping the specified columns, if any. Otherwise, returns the full schema of the table.
    """
    
    df = spark.read.table(table_name)
    if len(columns_to_drop) > 0:
        schema = df.drop(*columns_to_drop).schema
    else:
        schema = df.schema
    return schema

# COMMAND ----------

def applyTransformFunction(df, function_name, parameters):
    """ 
    Applies a transformation function to a Spark DataFrame using the provided function name and parameters. 
    
    Parameters: 
    ---------- 
        df : pyspark.sql.DataFrame 
            The DataFrame on which the transformation function will be applied. 
        function_name : function 
            The transformation function to be applied to the DataFrame. This should be a callable that takes a DataFrame as its first argument and applies specific transformations. 
        parameters : list
            A list of parameters to be passed to the transformation function, in addition to the DataFrame. 
    Returns: 
    ------- 
        pyspark.sql.DataFrame 
            A transformed Spark DataFrame after the transformation function has been applied.
    """

    transformed_df = df.transform(function_name, *parameters)
    return transformed_df
