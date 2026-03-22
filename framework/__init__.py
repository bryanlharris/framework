from framework.bronze import bronze_functions
from framework.silver import silver_functions
from framework.transform import transform_registry

function_registries = {
    "bronze": bronze_functions,
    "silver": silver_functions,
    "transform": transform_registry,
}

__all__ = ["function_registries"]
