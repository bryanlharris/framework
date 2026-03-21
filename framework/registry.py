from framework.bronze import bronze_functions
from framework.silver import silver_functions
from framework.gold import gold_functions
from framework.transforms import transform_registry

function_registries = {
    "bronze": bronze_functions,
    "silver": silver_functions,
    "gold": gold_functions,
    "transforms": transform_registry,
}

__all__ = ["function_registries"]
