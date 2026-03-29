from pathlib import Path


def create(spark, settings):
    view_source_path = settings["view_source_path"]
    sql = Path(view_source_path).read_text(encoding="utf-8")
    spark.sql(sql)
