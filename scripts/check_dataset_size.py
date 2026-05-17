"""Validate that the configured NYC TLC dataset selection meets project thresholds."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi_pipeline.config import DEFAULT_CONFIG_PATH, DEFAULT_ENV_FILE, load_config
from nyc_taxi_pipeline.ingestion.dataset_size import (
    DatasetThresholdError,
    check_dataset_size,
)
from nyc_taxi_pipeline.ingestion.source_discovery import discover_sources_from_config
from nyc_taxi_pipeline.logging_utils import configure_logging, get_logger, log_event


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument(
        "--skip-head",
        action="store_true",
        help="Skip HTTP HEAD checks and rely on record-count metadata only.",
    )
    return parser.parse_args()


def main() -> int:
    """Run dataset threshold validation."""
    args = parse_args()
    config = load_config(config_path=args.config, env_file=args.env_file)
    configure_logging(config.runtime.log_level)
    logger = get_logger("nyc_taxi_pipeline.check_dataset_size")
    sources = discover_sources_from_config(config.dataset)

    try:
        result = check_dataset_size(
            sources,
            config.dataset,
            fetch_remote_size=not args.skip_head,
            logger=logger,
        )
    except DatasetThresholdError as exc:
        log_event(
            logger,
            "dataset_threshold_failed",
            "check_dataset_size",
            level=logging.ERROR,
            error=str(exc),
        )
        return 1

    log_event(
        logger,
        "dataset_threshold_passed",
        "check_dataset_size",
        source_count=result.source_count,
        total_raw_bytes=result.total_raw_bytes,
        total_record_count=result.total_record_count,
        min_raw_bytes=result.min_raw_bytes,
        min_record_count=result.min_record_count,
        sample_mode=result.sample_mode,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
