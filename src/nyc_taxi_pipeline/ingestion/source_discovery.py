"""NYC TLC source file discovery."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from urllib.parse import urljoin

from nyc_taxi_pipeline.config import DatasetConfig

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
DECEMBER = 12


@dataclass(frozen=True)
class SourceFile:
    """One source file selected for ingestion."""

    year: int
    month: int
    file_name: str
    source_url: str

    @property
    def month_key(self) -> str:
        """Return the month key in `YYYY-MM` format."""
        return f"{self.year:04d}-{self.month:02d}"


def parse_month(month_value: str) -> date:
    """Parse and validate a `YYYY-MM` month string."""
    if not MONTH_PATTERN.match(month_value):
        msg = f"Invalid month '{month_value}'. Expected format YYYY-MM."
        raise ValueError(msg)
    year_text, month_text = month_value.split("-", 1)
    return date(int(year_text), int(month_text), 1)


def iter_months(start_month: str, end_month: str) -> list[date]:
    """Return all months between start and end, inclusive."""
    current = parse_month(start_month)
    end = parse_month(end_month)
    if current > end:
        msg = f"start_month '{start_month}' must be <= end_month '{end_month}'."
        raise ValueError(msg)

    months: list[date] = []
    while current <= end:
        months.append(current)
        next_year = current.year + 1 if current.month == DECEMBER else current.year
        next_month = 1 if current.month == DECEMBER else current.month + 1
        current = date(next_year, next_month, 1)
    return months


def build_yellow_taxi_file_name(year: int, month: int) -> str:
    """Build the standard NYC TLC Yellow Taxi Parquet file name."""
    return f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def discover_yellow_taxi_sources(
    start_month: str,
    end_month: str,
    source_base_url: str,
) -> list[SourceFile]:
    """Generate NYC TLC Yellow Taxi Parquet URLs for a month range."""
    base_url = source_base_url.rstrip("/") + "/"
    sources: list[SourceFile] = []
    for month in iter_months(start_month, end_month):
        file_name = build_yellow_taxi_file_name(month.year, month.month)
        sources.append(
            SourceFile(
                year=month.year,
                month=month.month,
                file_name=file_name,
                source_url=urljoin(base_url, file_name),
            )
        )
    return sources


def discover_sources_from_config(dataset_config: DatasetConfig) -> list[SourceFile]:
    """Generate source files from the resolved pipeline dataset config."""
    return discover_yellow_taxi_sources(
        dataset_config.start_month,
        dataset_config.end_month,
        dataset_config.base_url,
    )
