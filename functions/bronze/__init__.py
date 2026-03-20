from functions.bronze.bronze_function_to_delta_table import bronze_function_to_delta_table
from functions.bronze.example_1 import example_1

STATIC_FUNCTIONS = {
    "bronze_function_to_delta_table": bronze_function_to_delta_table,
    "example_1": example_1,
}

__all__ = ["STATIC_FUNCTIONS", "bronze_function_to_delta_table", "example_1"]
