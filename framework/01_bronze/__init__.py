from .pipelines import example_1, example_2
from .ingest import ingest_bronze

bronze_functions = {
    "ingest_bronze": ingest_bronze,
    "example_1": example_1,
    "example_2": example_2,
    "framework.ingest.ingest_bronze": ingest_bronze,
    "framework.bronze.functions.example_1": example_1,
    "framework.bronze.functions.example_2": example_2,
}

__all__ = [
    "bronze_functions",
    "ingest_bronze",
    "example_1",
    "example_2",
]
