

from delta.tables import DeltaTable


def streaming_read(source_type, readstream_options, source, schema = None):
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


def streaming_write(df, target_table, table_format, output_mode, query_name, trigger_type, writestream_options):
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


def truncateAndUpsertToDeltaWithKeys(mergeKeys, destinationTable):
    def _do_upsert(microBatchDF, batchId):
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


def upsertToDeltaWithPK(sourcePK, destinationTable, destinationPK):
    def _do_upsert(microBatchDF, batchId):
        deltaTable = DeltaTable.forName(spark, destinationTable)
        (
            deltaTable.alias("d")
            .merge(microBatchDF.alias("s"), f"s.{sourcePK} = d.{destinationPK}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    return _do_upsert
