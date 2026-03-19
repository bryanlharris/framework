# Databricks notebook source
def streaming_read(source_type, readstream_options, source, schema = None):
    """ 
    Reads a streaming data source into a Spark DataFrame using the specified schema, source format, options, and source path. 
    
    Parameters: 
    ---------- 
        schema : pyspark.sql.types.StructType 
            The schema of the data to be read. This defines the structure of the incoming data. 
        source_type : str 
            Streaming source type (e.g., "cloudfiles", "table")
        readstream_options : dict 
            A dictionary of options to configure the streaming read (e.g., 'maxFilesPerTrigger', 'header' for CSV). 
        source : str 
            The path/table to the data source to read the streaming data from (e.g., a directory path for files or Kafka topic or a table). 
    Returns: 
    ------- 
        pyspark.sql.DataFrame 
            A Spark DataFrame representing the streaming data.
    """

    if source_type.lower() == "cloudfiles" and schema:
        df = (
            spark.readStream
            .schema(schema)
            .format(source_type)
            .options(**readstream_options)
            .load(source)
        )
    elif source_type.lower() == "cloudfiles" and not schema:
        df = (
            spark.readStream
            .format(source_type)
            .options(**readstream_options)
            .load(source)
        )
    elif source_type.lower() == "table" and readstream_options:
        df = (
            spark.readStream
            .options(**readstream_options)
            .table(source)
        )
    elif source_type.lower() == "table" and not readstream_options:
        df = (
            spark.readStream
            .table(source)
        )
    return df

# COMMAND ----------

def streaming_write(df, target_table, table_format, output_mode, query_name, trigger_type, writestream_options):
    """ 
    Writes a streaming Spark DataFrame to a specified target table with the given format, output mode, and write stream options. 
    
    Parameters: 
    ---------- 
        df : pyspark.sql.DataFrame 
            The streaming DataFrame to be written to the target table.
        target_table : str 
            The name of the target table where the data will be written. 
        table_format : str 
            The format of the table (e.g., "parquet", "delta"). 
        output_mode : str 
            The output mode of the streaming write operation. Common modes include: 
            - "append": Only new rows are appended to the table. 
            - "complete": All rows are written to the table every time. 
            - "update": Only updated rows are written to the table.
        query_name : str 
            The name of the query, useful for monitoring or managing the query. 
        trigger_type : dict 
            A dictionary specifying the trigger type for the stream. Common triggers include: 
            - {"availableNow": True} for processing all available data once. 
            - {"continuous": "1 second"} for continuous processing every second. 
            - {"processingTime": "10 seconds"} for processing batches every 10 seconds. 
        writestream_options : dict 
            A dictionary of options to configure the streaming write (e.g., "checkpointLocation" for state management). 
    Returns: 
    ------- 
        pyspark.sql.streaming.StreamingQuery 
            A `StreamingQuery` object that represents the streaming query. This can be used to monitor the status or stop the query.
    """
    # def writeBatch(batch_df, batch_id):
    #     batch_df.write
    #     .saveAsTable(target_table)

    # query = (df.writeStream)
    #             .format(table_format)
    #             .foreachBatch(writeBatch)
    #             .outputMode(output_mode)
    #             .queryName(query_name)
    #             .options(**writestream_options)
    #             .trigger(**trigger_type)

    query = (
        df.writeStream
        .format(table_format)
        .outputMode(output_mode)
        .queryName(query_name)
        .trigger(**trigger_type)
        .options(**writestream_options)
        .toTable(target_table)
    )
    return query

# COMMAND ----------

def truncateAndUpsertToDeltaWithKeysSQL(mergeKeys, destinationTable):
    def _do_upsert(microBatchDF, batchId):
        merge_condition = " AND ".join([f"source.{col} = target.{col}" for col in mergeKeys])

        microBatchDF.createOrReplaceTempView("microBatchDF")

        microBatchDF.sparkSession.sql(f"""
            MERGE INTO {destinationTable} AS target
            USING microBatchDF AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)

    return _do_upsert

# COMMAND ----------

def truncateAndUpsertToDeltaWithKeys(mergeKeys, destinationTable):
    def _do_upsert(microBatchDF, batchId):
        from delta.tables import DeltaTable
        deltaTable = DeltaTable.forName(spark, destinationTable)

        merge_condition = " AND ".join([f"s.{col} = d.{col}" for col in mergeKeys])
        (
            deltaTable.alias("d")
            .merge(microBatchDF.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
    return _do_upsert

# COMMAND ----------

def upsertToDeltaWithPK(sourcePK, destinationTable, destinationPK):
    """ 
    Creates a function to perform an upsert (merge) operation on a Delta table using a micro-batch DataFrame.

    Args:
        sourcePk (str): The primary key column name in the source DataFrame.
        destinationTable (str): The name of the destination Delta table.
        destinationPK (str): The primary key column name in the destination Delta table.
    Returns:
        funtion: A function _do_upsert that takes a micro-batch DataFrame (microBatchDF) and a batch ID (batchID) as arguments, and performs the upsert operation on the specified Delta table.

        The _do_upsert function performs the following steps:
        - Uses the merge operation to match records between the source and destination based on the specified primary keys.
        - Updates matching records with values from the source DataFrame.
        - Inserts new records from the source DataFrame into the destination Delta table if they do not already exist.
    """
    def _do_upsert(microBatchDF, batchId):
        from delta.tables import DeltaTable
        deltaTable = DeltaTable.forName(spark, destinationTable)
        (
            deltaTable.alias("d")
            .merge(microBatchDF.alias("s"), f"s.{sourcePK} = d.{destinationPK}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    return _do_upsert