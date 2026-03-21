from functions.bronze import bronze_functions
from functions.silver import silver_functions

function_registries = {
    "bronze": bronze_functions,
    "silver": silver_functions,
}

__all__ = ["function_registries"]
