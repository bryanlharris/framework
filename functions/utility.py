"""Utility helpers for the ``functions`` package.

This module contains importable Python helpers that are shared across the
package. It is written for direct package imports in standard Python modules.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def get_latest_file_path(volume_path: str, date_pattern: str) -> str | None:
    """Return the latest filename in ``volume_path`` that matches ``date_pattern``.

    The function expects ``date_pattern`` to contain a capture group whose value
    sorts in the same order as recency, such as ``YYYYMMDD``.
    """
    files = dbutils.fs.ls(volume_path)

    def extract_datetime(filename: str) -> str | None:
        match = re.search(date_pattern, filename)
        if match:
            return match.group(1)
        return None

    files_with_dates = [
        (file_info.path, extract_datetime(file_info.name))
        for file_info in files
        if extract_datetime(file_info.name) is not None
    ]
    latest_file = max(files_with_dates, key=lambda item: item[1], default=None)

    if latest_file is None:
        return None

    return latest_file[0].split("/")[-1]



def read_json_and_decode(workspace_path: str | Path) -> Any:
    """Read JSON from a local path or simple glob and decode it."""
    if isinstance(workspace_path, Path):
        resolved_path = workspace_path
    else:
        workspace_path = str(workspace_path)
        if "*" in workspace_path:
            matches = sorted(Path().glob(workspace_path))
            if len(matches) != 1:
                raise ValueError(
                    "Expected exactly one file after interpreting globs in workspace path."
                )
            resolved_path = matches[0]
        else:
            candidate_path = Path(workspace_path)
            resolved_path = (
                candidate_path
                if candidate_path.is_absolute()
                else Path.cwd() / candidate_path
            )

    return json.loads(resolved_path.read_text(encoding="utf-8"))
