def streaming_read(source_type, readstream_options, source, schema=None):
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
