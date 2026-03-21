from functions.bronze import bronze_functions
from functions.silver import silver_functions

FUNCTION_REGISTRIES = {
    "bronze": bronze_functions,
    "silver": silver_functions,
}

__all__ = ["FUNCTION_REGISTRIES"]
