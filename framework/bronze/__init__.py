from framework.bronze.pipelines import run_file_ingest

bronze_functions = {
    "framework.bronze.pipelines.run_file_ingest": run_file_ingest,
}

__all__ = [
    "bronze_functions",
    "run_file_ingest",
]
