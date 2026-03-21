from functions.bronze import bronze_functions
from functions.silver import silver_functions
from functions.transforms import transform_registry

function_registries = {
    "bronze": bronze_functions,
    "silver": silver_functions,
    "transforms": transform_registry,
}

__all__ = ["function_registries"]
