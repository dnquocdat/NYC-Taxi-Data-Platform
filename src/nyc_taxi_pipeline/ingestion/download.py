"""Source download and staging helpers."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import requests


def stage_source_file(
    source_url: str,
    staging_dir: Path,
    *,
    timeout_seconds: int = 120,
    chunk_size: int = 1024 * 1024,
) -> Path:
    """Download an HTTP source file into a shared staging directory if needed.

    Spark workers must be able to read the staged path, so Docker Compose mounts the same
    directory into Airflow and Spark containers at `/opt/shared/source_cache`.
    Non-HTTP paths are returned unchanged.
    """
    parsed = urlparse(source_url)
    if parsed.scheme == "file":
        return Path(url2pathname(parsed.path))
    if parsed.scheme not in {"http", "https"}:
        return Path(source_url)

    staging_dir.mkdir(parents=True, exist_ok=True)
    file_name = Path(parsed.path).name
    if not file_name:
        msg = f"Cannot derive file name from source URL: {source_url}"
        raise ValueError(msg)

    target_path = staging_dir / file_name
    if target_path.exists() and target_path.stat().st_size > 0:
        return target_path

    temporary_path = target_path.with_suffix(target_path.suffix + ".part")
    with requests.get(source_url, stream=True, timeout=timeout_seconds) as response:
        response.raise_for_status()
        with temporary_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    handle.write(chunk)

    temporary_path.replace(target_path)
    return target_path
