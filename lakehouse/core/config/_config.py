import json
from pathlib import Path


def _is_silver_table(table: str) -> bool:
    parts = table.split(".")
    return len(parts) == 3 and parts[1] == "silver"


def _topo_sort(items: list[dict], dep_map: dict[str, str | None]) -> list[dict]:
    """
    Topologically sort items so each item appears after its dependency.

    dep_map: maps settings_file -> source silver destination_table (or None).
    Raises ValueError if a cycle is detected.
    """
    dest_to_item = {}
    for item in items:
        path = Path(item["settings_file"])
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            dest = data.get("destination_table")
            if dest:
                dest_to_item[dest] = item
        except Exception:
            pass

    visited = set()
    in_stack = set()
    result = []

    def visit(item: dict):
        sf = item["settings_file"]
        if sf in in_stack:
            raise ValueError(f"Cycle detected involving {sf}")
        if sf in visited:
            return
        in_stack.add(sf)
        dep_table = dep_map.get(sf)
        if dep_table and dep_table in dest_to_item:
            visit(dest_to_item[dep_table])
        in_stack.discard(sf)
        visited.add(sf)
        result.append(item)

    for item in items:
        visit(item)

    return result


def scan_settings_folder(color: str, dependents: bool | None = None) -> list[dict]:
    """
    Scan settings/{color}/ for active .json files.

    Returns list of dicts with 'settings_file' key.

    For color='silver', dependents controls filtering:
      - None (default): return all silver settings (original behavior)
      - False: return only silver settings whose source_table is not a silver table
      - True: return only silver settings whose source_table is a silver table,
              topologically sorted so dependencies come first. Raises ValueError on cycles.
    """
    folder = Path(f"settings/{color}")
    all_items = []
    dep_map: dict[str, str | None] = {}

    for path in sorted(folder.iterdir()):
        if path.is_file() and path.name.endswith(".json"):
            item = {"settings_file": str(path)}
            all_items.append(item)
            if color == "silver":
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    source = data.get("source_table", "")
                    dep_map[str(path)] = source if _is_silver_table(source) else None
                except Exception:
                    dep_map[str(path)] = None

    if color != "silver" or dependents is None:
        return all_items

    if dependents is False:
        return [item for item in all_items if dep_map.get(item["settings_file"]) is None]

    # dependents=True: silver tables that depend on another silver table
    dependent_items = [item for item in all_items if dep_map.get(item["settings_file"]) is not None]
    return _topo_sort(dependent_items, dep_map)


def extract_destination_tables(color: str) -> list[dict]:
    """
    Read all active .json files in settings/{color}/ and extract destination_table.

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
