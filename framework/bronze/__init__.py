from framework.bronze.pipelines import example_1, example_2, run_file_ingest

bronze_functions = {
    "framework.bronze.pipelines.run_file_ingest": run_file_ingest,
    "framework.bronze.pipelines.example_1": example_1,
    "framework.bronze.pipelines.example_2": example_2,
}

__all__ = [
    "bronze_functions",
    "run_file_ingest",
    "example_1",
    "example_2",
]
