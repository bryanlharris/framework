from framework.bronze.pipelines import example_1, example_2
from framework.bronze.ingest import ingest_bronze

bronze_functions = {
    "ingest_bronze": ingest_bronze,
    "example_1": example_1,
    "example_2": example_2,
"framework.bronze.ingest.ingest_bronze": ingest_bronze,
    "framework.bronze.pipelines.example_1": example_1,
    "framework.bronze.pipelines.example_2": example_2,
}

__all__ = [
    "bronze_functions",
    "ingest_bronze",
    "example_1",
    "example_2",
]
