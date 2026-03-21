from pyspark.sql.functions import current_timestamp, expr


def example_2(settings):
    dst_table_name          = settings["dst_table_name"]
    catalog_name            = settings["dst_table_name"].split(".")[0]
    bronze_schema           = settings["dst_table_name"].split(".")[1]
    table                   = settings["dst_table_name"].split(".")[2]
    readStreamOptions       = settings["readStreamOptions"]
    writeStreamOptions      = settings["writeStreamOptions"]
    readStream_load         = settings["readStream_load"]
    writeStream_format      = settings["writeStream_format"]
    writeStream_outputMode  = settings["writeStream_outputMode"]

    (
        spark.readStream.format("cloudfiles")
        .options(**readStreamOptions)
        .load(readStream_load)
        .withColumn("ingest_time", current_timestamp())
        .withColumn("source_metadata", expr("_metadata"))
        .writeStream.format(writeStream_format)
        .options(**writeStreamOptions)
        .outputMode(writeStream_outputMode)
        .trigger(availableNow=True)
        .table(f"{catalog_name}.{bronze_schema}.{table}")
    )
