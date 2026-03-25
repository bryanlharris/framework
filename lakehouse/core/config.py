import json
from pathlib import Path


def scan_settings_folder(color: str) -> list[dict]:
    """Scan settings/{color}/ for active .json files.

    Returns list of dicts with 'settings_file' key.
    """
    folder = Path(f"settings/{color}")
    result = []
    for path in sorted(folder.iterdir()):
        if path.is_file() and path.name.endswith(".json"):
            result.append({"settings_file": str(path)})
    return result


def extract_destination_tables(color: str) -> list[dict]:
    """Read all active .json files in settings/{color}/ and extract destination_table.

    Skips files that lack the destination_table field.
    Returns list of dicts with 'full_table_name' key.
    """
    folder = Path(f"settings/{color}")
    result = []
    for path in sorted(folder.iterdir()):
        if path.is_file() and path.name.endswith(".json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if "destination_table" in data:
                    result.append({"full_table_name": data["destination_table"]})
            except Exception:
                pass
    return result
