from functions.bronze.bronze_function_to_delta_table import bronze_function_to_delta_table
from functions.bronze.example_1 import example_1
from functions.bronze.example_2 import example_2

bronze_functions = {
    "bronze_function_to_delta_table": bronze_function_to_delta_table,
    "functions.bronze.bronze_function_to_delta_table.bronze_function_to_delta_table": bronze_function_to_delta_table,
    "example_1": example_1,
    "functions.bronze.example_1.example_1": example_1,
    "example_2": example_2,
    "functions.bronze.example_2.example_2": example_2,
}

__all__ = [
    "bronze_functions",
    "bronze_function_to_delta_table",
    "example_1",
    "example_2",
]
