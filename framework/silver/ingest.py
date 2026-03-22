from pyspark.sql.functions import col, current_timestamp

from framework.core.merge import upsertByPK
from framework.transform.metadata import add_row_sha_checksum
from framework.transform.columns import cast_data_types, rename_columns


def from_table(settings):
    source_table        = settings["source_table"]
    destination_table   = settings["destination_table"]
    column_map          = settings["column_map"]
    data_type_map       = settings["data_type_map"]
    writeStream_options = settings["writeStream_options"]
    pk                  = settings["pk"]["name"]
    pk_columns          = settings["pk"]["columns"]
    pk_columns_str      = ",".join(pk_columns) if isinstance(pk_columns, list) else pk_columns
    readStream_options  = settings["readStream_options"]

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    df = (
        spark.readStream
        .options(**readStream_options)
        .table(source_table)
        .drop("ingest_time")
        .transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .select(
            "*",
            col("source_metadata.file_path").alias("file_path"),
            col("source_metadata.file_modification_time").alias(
                "file_modification_time"
            ),
            current_timestamp().alias("ingest_time"),
        )
    )

    df = add_row_sha_checksum(df, col_name=pk, columns=pk_columns_str, bitlength=256)

    (
        df.writeStream
        .queryName(destination_table)
        .format("delta")
        .options(**writeStream_options)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(upsertByPK(pk, destination_table, pk))
        .start()
    )
