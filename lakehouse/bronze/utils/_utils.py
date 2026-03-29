import re


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
