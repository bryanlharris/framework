import importlib
import sys

# Load subpackages in dependency order and register non-numbered aliases.

# 1. core (no internal framework deps)
_core = importlib.import_module("framework.00_core")
sys.modules["framework.core"] = _core
for _sub in ["utils", "streaming", "merge"]:
    sys.modules[f"framework.core.{_sub}"] = importlib.import_module(f"framework.00_core.{_sub}")

# 2. transform (depends on core only)
_transform = importlib.import_module("framework.05_transform")
sys.modules["framework.transform"] = _transform
for _sub in ["metadata", "columns"]:
    sys.modules[f"framework.transform.{_sub}"] = importlib.import_module(f"framework.05_transform.{_sub}")

# 3. bronze (depends on core and transform)
_bronze = importlib.import_module("framework.01_bronze")
sys.modules["framework.bronze"] = _bronze
for _sub in ["ingest", "pipelines"]:
    sys.modules[f"framework.bronze.{_sub}"] = importlib.import_module(f"framework.01_bronze.{_sub}")

# 4. silver (depends on core and transform)
_silver = importlib.import_module("framework.02_silver")
sys.modules["framework.silver"] = _silver
for _sub in ["pipelines"]:
    sys.modules[f"framework.silver.{_sub}"] = importlib.import_module(f"framework.02_silver.{_sub}")
