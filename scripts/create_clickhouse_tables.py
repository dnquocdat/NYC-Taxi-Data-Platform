"""Create ClickHouse serving tables from the project SQL file."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi_pipeline.config import DEFAULT_CONFIG_PATH, DEFAULT_ENV_FILE, load_config
from nyc_taxi_pipeline.logging_utils import configure_logging, get_logger, log_event
from nyc_taxi_pipeline.spark.clickhouse_load import execute_clickhouse_query

CLICKHOUSE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument(
        "--sql-file",
        type=Path,
        default=PROJECT_ROOT / "scripts" / "create_clickhouse_tables.sql",
    )
    return parser.parse_args()


def split_sql_statements(sql_text: str) -> list[str]:
    """Split simple semicolon-terminated SQL statements."""
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def render_clickhouse_ddl(sql_template: str, *, database: str) -> str:
    """Render the ClickHouse DDL template with a validated database identifier."""
    if not CLICKHOUSE_IDENTIFIER_PATTERN.match(database):
        msg = f"Invalid ClickHouse database identifier: {database!r}"
        raise ValueError(msg)
    return sql_template.format(database=database)


def main() -> int:
    """Execute ClickHouse DDL statements with env-based credentials."""
    args = parse_args()
    config = load_config(config_path=args.config, env_file=args.env_file)
    configure_logging(config.runtime.log_level)
    logger = get_logger("nyc_taxi_pipeline.create_clickhouse_tables")
    ddl = render_clickhouse_ddl(
        args.sql_file.read_text(encoding="utf-8"),
        database=config.clickhouse.database,
    )
    statements = split_sql_statements(ddl)

    for index, statement in enumerate(statements, start=1):
        execute_clickhouse_query(config.clickhouse, statement)
        log_event(
            logger,
            "clickhouse_ddl_executed",
            "create_clickhouse_tables",
            statement_index=index,
            statement_count=len(statements),
        )

    log_event(
        logger,
        "clickhouse_tables_created",
        "create_clickhouse_tables",
        statement_count=len(statements),
        database=config.clickhouse.database,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
