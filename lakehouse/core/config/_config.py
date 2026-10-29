from pathlib import Path

from lakehouse.core.utils import read_json_and_decode


def _is_silver_table(table: str) -> bool:
    parts = table.split(".")
    return len(parts) == 3 and parts[1] == "silver"


ENVIRONMENTS = ("free", "test", "dev", "staging", "prod")


def _check_environment(environment: str) -> None:
    if environment not in ENVIRONMENTS:
        raise ValueError(f"environment {environment!r} must be one of {list(ENVIRONMENTS)}")


TABLE_KEYS = ("destination_table", "source_table")


def _check_catalog_prefix(path) -> None:
    """
    Require every table name in the settings file to start with ${catalog_prefix}.

    Checked on the raw file, before substitution: with an empty prefix (free,
    prod) a missing token would otherwise look identical to a correct one.
    Without it, dev, staging and prod would all write to the same catalog.
    """
    raw = read_json_and_decode(path, keep_tokens=True)
    names = [raw[k] for k in TABLE_KEYS if k in raw] + list(raw.get("depends_on", []))
    missing = [n for n in names if not str(n).startswith("${catalog_prefix}")]
    if missing:
        raise ValueError(
            f"{path}: table names must start with ${{catalog_prefix}} so each "
            f"environment writes to its own catalog: {missing}"
        )


def _runs_in(data: dict, environment: str, path) -> bool:
    """
    True if the settings' required "environments" list includes environment.

    Raises if the list is missing, empty, or names an unknown environment, so
    every settings file states exactly where it runs.
    """
    environments = data.get("environments")
    if not isinstance(environments, list) or not environments:
        raise ValueError(f'{path}: "environments" is required, e.g. ["free", "dev", "staging", "prod"]')
    unknown = [e for e in environments if e not in ENVIRONMENTS]
    if unknown:
        raise ValueError(f'{path}: unknown environment(s) {unknown}; use {list(ENVIRONMENTS)}')
    return environment in environments


def runs_in(data: dict, environment: str, path) -> bool:
    """
    True if a settings file's "environments" list includes environment.

    For jobs other than ingest that keep their own settings folder (fetch_url).
    Raises on an unknown environment or a bad "environments" list.
    """
    _check_environment(environment)
    return _runs_in(data, environment, path)


def _topo_sort(items: list[dict], dep_map: dict[str, str | None], catalog_prefix: str = "") -> list[dict]:
    """
    Topologically sort items so each item appears after its dependency.

    dep_map: maps settings_file -> source silver destination_table (or None).
    Raises ValueError if a cycle is detected.
    """
    dest_to_item = {}
    for item in items:
        try:
            data = read_json_and_decode(item["settings_file"], catalog_prefix)
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


def scan_settings_folder(
    color: str,
    dependents: bool | None = None,
    *,
    catalog_prefix: str,
    environment: str,
) -> list[dict]:
    """
    Scan settings/{color}/ for active .json files that run in environment.

    Returns list of dicts with 'settings_file' and 'catalog_prefix' keys.

    For color='silver', dependents controls filtering:
      - None (default): return all silver settings (original behavior)
      - False: return only silver settings whose source_table is not a silver table
      - True: return only silver settings whose source_table is a silver table,
              topologically sorted so dependencies come first. Raises ValueError on cycles.
    """
    _check_environment(environment)
    folder = Path(f"settings/{color}")
    all_items = []
    dep_map: dict[str, str | None] = {}

    for path in sorted(folder.iterdir()):
        if path.is_file() and path.name.endswith(".json"):
            # Every file, even ones skipped here, so a mistake fails in free
            # before it can reach a work environment.
            _check_catalog_prefix(path)
            data = read_json_and_decode(path, catalog_prefix)
            if not _runs_in(data, environment, path):
                continue
            item = {"settings_file": str(path), "catalog_prefix": catalog_prefix}
            all_items.append(item)
            if color == "silver":
                source = data.get("source_table", "")
                depends_on = data.get("depends_on", [])
                silver_deps = [t for t in depends_on if _is_silver_table(t)]
                if _is_silver_table(source):
                    dep_map[str(path)] = source
                elif silver_deps:
                    dep_map[str(path)] = silver_deps[0]
                else:
                    dep_map[str(path)] = None

    if color != "silver" or dependents is None:
        return all_items

    if dependents is False:
        return [item for item in all_items if dep_map.get(item["settings_file"]) is None]

    # dependents=True: silver tables that depend on another silver table
    dependent_items = [item for item in all_items if dep_map.get(item["settings_file"]) is not None]
    return _topo_sort(dependent_items, dep_map, catalog_prefix)


def extract_destination_tables(color: str, catalog_prefix: str, environment: str) -> list[dict]:
    """
    Read all active .json files in settings/{color}/ that run in environment and
    extract destination_table.

    Skips files that lack the destination_table field.
    Returns list of dicts with 'full_table_name', 'settings_file' and 'catalog_prefix' keys.
    """
    _check_environment(environment)
    folder = Path(f"settings/{color}")
    result = []
    for path in sorted(folder.iterdir()):
        if path.is_file() and path.name.endswith(".json"):
            data = read_json_and_decode(path, catalog_prefix)
            if _runs_in(data, environment, path) and "destination_table" in data:
                result.append({
                    "full_table_name": data["destination_table"],
                    "settings_file": str(path),
                    "catalog_prefix": catalog_prefix,
                })
    return result


def skipped_settings(color: str, catalog_prefix: str, environment: str) -> list[str]:
    """Active .json files in settings/{color}/ that do not run in environment."""
    _check_environment(environment)
    folder = Path(f"settings/{color}")
    return [
        str(path)
        for path in sorted(folder.iterdir())
        if path.is_file() and path.name.endswith(".json")
        and not _runs_in(read_json_and_decode(path, catalog_prefix), environment, path)
    ]
