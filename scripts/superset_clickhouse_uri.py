"""Render the Superset SQLAlchemy URI for the local ClickHouse service."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi_pipeline.config import DEFAULT_CONFIG_PATH, DEFAULT_ENV_FILE, load_config


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument(
        "--show-password",
        action="store_true",
        help="Render the full password for copy/paste into Superset.",
    )
    return parser.parse_args()


def build_superset_clickhouse_uri(
    *,
    user: str,
    password: str | None,
    host: str,
    port: int,
    database: str,
    show_password: bool,
) -> str:
    """Build a clickhouse-connect SQLAlchemy URI for Superset."""
    encoded_user = quote(user, safe="")
    encoded_password = quote(password or "", safe="") if show_password else "********"
    return f"clickhousedb://{encoded_user}:{encoded_password}@{host}:{port}/{database}"


def main() -> int:
    """Print the Superset ClickHouse URI."""
    args = parse_args()
    config = load_config(config_path=args.config, env_file=args.env_file)
    uri = build_superset_clickhouse_uri(
        user=config.clickhouse.user,
        password=config.clickhouse.password,
        host=config.clickhouse.host,
        port=config.clickhouse.http_port,
        database=config.clickhouse.database,
        show_password=args.show_password,
    )
    sys.stdout.write(f"{uri}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
