from functions.bronze.example_1 import example_1
from functions.bronze.example_2 import example_2
from functions.ingest import ingest_bronze

bronze_functions = {
    "ingest_bronze": ingest_bronze,
    "functions.ingest.ingest_bronze": ingest_bronze,
    "example_1": example_1,
    "functions.bronze.example_1.example_1": example_1,
    "example_2": example_2,
    "functions.bronze.example_2.example_2": example_2,
}

__all__ = [
    "bronze_functions",
    "ingest_bronze",
    "example_1",
    "example_2",
]
