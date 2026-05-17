"""Check that configured NYC TLC source files are reachable."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi_pipeline.config import DEFAULT_CONFIG_PATH, DEFAULT_ENV_FILE, load_config
from nyc_taxi_pipeline.ingestion.source_discovery import discover_sources_from_config
from nyc_taxi_pipeline.logging_utils import configure_logging, get_logger, log_event

HTTP_BAD_REQUEST = 400


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-delay-seconds", type=float, default=2.0)
    return parser.parse_args()


def check_url_available(
    url: str,
    timeout_seconds: int,
    *,
    retries: int = 3,
    retry_delay_seconds: float = 2.0,
) -> None:
    """Raise a clear error if a source URL cannot be reached."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            status = _request_status(url, timeout_seconds, method="HEAD")
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            try:
                status = _request_status(
                    url,
                    timeout_seconds,
                    method="GET",
                    headers={"Range": "bytes=0-0"},
                )
            except (HTTPError, URLError, TimeoutError) as fallback_exc:
                last_error = fallback_exc
                if attempt < retries:
                    time.sleep(retry_delay_seconds)
                    continue
                msg = f"Source URL is not available: {url}. Error: {last_error}"
                raise RuntimeError(msg) from fallback_exc
        break

    if status >= HTTP_BAD_REQUEST:
        msg = f"Source URL returned HTTP {status}: {url}"
        raise RuntimeError(msg)


def _request_status(
    url: str,
    timeout_seconds: int,
    *,
    method: str,
    headers: dict[str, str] | None = None,
) -> int:
    request = Request(url, headers=headers or {}, method=method)
    with urlopen(request, timeout=timeout_seconds) as response:
        return int(getattr(response, "status", 200))


def main() -> int:
    """Validate source availability for the configured month range."""
    args = parse_args()
    config = load_config(config_path=args.config, env_file=args.env_file)
    configure_logging(config.runtime.log_level)
    logger = get_logger("nyc_taxi_pipeline.check_source_available")
    sources = discover_sources_from_config(config.dataset)

    for source in sources:
        try:
            check_url_available(
                source.source_url,
                args.timeout_seconds,
                retries=args.retries,
                retry_delay_seconds=args.retry_delay_seconds,
            )
        except RuntimeError as exc:
            log_event(
                logger,
                "source_unavailable",
                "check_source_available",
                level=logging.ERROR,
                source_url=source.source_url,
                error=str(exc),
            )
            return 1
        log_event(
            logger,
            "source_available",
            "check_source_available",
            source_url=source.source_url,
            source_file=source.file_name,
        )

    log_event(
        logger,
        "source_availability_check_completed",
        "check_source_available",
        source_count=len(sources),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
