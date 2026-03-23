from framework.bronze import bronze_functions
from framework.silver import silver_functions

function_registries = {
    "bronze": bronze_functions,
    "silver": silver_functions,
}

__all__ = ["function_registries"]
