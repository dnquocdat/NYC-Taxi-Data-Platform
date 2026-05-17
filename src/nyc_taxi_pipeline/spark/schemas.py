"""Expected source schemas and column mappings."""

from __future__ import annotations

import re
from dataclasses import dataclass


class SchemaValidationError(ValueError):
    """Raised when required source columns are missing."""


@dataclass(frozen=True)
class ColumnSpec:
    """Expected column mapping and type for a taxi trip field."""

    target_name: str
    aliases: tuple[str, ...]
    data_type: str
    required: bool = True


YELLOW_TAXI_COLUMN_SPECS: tuple[ColumnSpec, ...] = (
    ColumnSpec("vendor_id", ("vendor_id", "vendorid", "VendorID"), "int"),
    ColumnSpec(
        "pickup_datetime",
        ("pickup_datetime", "tpep_pickup_datetime"),
        "timestamp",
    ),
    ColumnSpec(
        "dropoff_datetime",
        ("dropoff_datetime", "tpep_dropoff_datetime"),
        "timestamp",
    ),
    ColumnSpec("passenger_count", ("passenger_count",), "double", required=False),
    ColumnSpec("trip_distance", ("trip_distance",), "double"),
    ColumnSpec("rate_code_id", ("rate_code_id", "ratecodeid", "RatecodeID"), "int", required=False),
    ColumnSpec("store_and_fwd_flag", ("store_and_fwd_flag",), "string", required=False),
    ColumnSpec(
        "pickup_location_id",
        ("pickup_location_id", "pulocationid", "PULocationID"),
        "int",
    ),
    ColumnSpec(
        "dropoff_location_id",
        ("dropoff_location_id", "dolocationid", "DOLocationID"),
        "int",
    ),
    ColumnSpec("payment_type_id", ("payment_type_id", "payment_type"), "int", required=False),
    ColumnSpec("fare_amount", ("fare_amount",), "double"),
    ColumnSpec("extra", ("extra",), "double", required=False),
    ColumnSpec("mta_tax", ("mta_tax",), "double", required=False),
    ColumnSpec("tip_amount", ("tip_amount",), "double", required=False),
    ColumnSpec("tolls_amount", ("tolls_amount",), "double", required=False),
    ColumnSpec("improvement_surcharge", ("improvement_surcharge",), "double", required=False),
    ColumnSpec("total_amount", ("total_amount",), "double"),
    ColumnSpec("congestion_surcharge", ("congestion_surcharge",), "double", required=False),
    ColumnSpec("airport_fee", ("airport_fee",), "double", required=False),
)

BRONZE_METADATA_COLUMNS = (
    "source_file",
    "source_url",
    "ingestion_timestamp",
    "batch_id",
    "dataset_year",
    "dataset_month",
)


def canonicalize_column_name(column_name: str) -> str:
    """Normalize a source column name for matching."""
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", column_name).strip("_").lower()
    return normalized


def resolve_column_mapping(available_columns: list[str] | tuple[str, ...]) -> dict[str, str]:
    """Map target snake_case names to available source column names.

    Optional missing columns are omitted. Missing required columns raise a clear error that lists
    every unresolved field and accepted aliases.
    """
    canonical_to_original = {
        canonicalize_column_name(column): column for column in available_columns
    }
    mapping: dict[str, str] = {}
    missing_required: list[str] = []

    for spec in YELLOW_TAXI_COLUMN_SPECS:
        matched_source = _match_spec(spec, canonical_to_original)
        if matched_source is not None:
            mapping[spec.target_name] = matched_source
        elif spec.required:
            aliases = ", ".join(spec.aliases)
            missing_required.append(f"{spec.target_name} (aliases: {aliases})")

    if missing_required:
        joined = "; ".join(missing_required)
        msg = f"Missing required NYC TLC source columns: {joined}"
        raise SchemaValidationError(msg)

    return mapping


def missing_optional_columns(available_columns: list[str] | tuple[str, ...]) -> list[str]:
    """Return optional target columns that are absent from the source schema."""
    mapping = resolve_column_mapping(available_columns)
    return [
        spec.target_name
        for spec in YELLOW_TAXI_COLUMN_SPECS
        if not spec.required and spec.target_name not in mapping
    ]


def required_target_columns() -> list[str]:
    """Return target columns that must exist after normalization."""
    return [spec.target_name for spec in YELLOW_TAXI_COLUMN_SPECS if spec.required]


def _match_spec(spec: ColumnSpec, canonical_to_original: dict[str, str]) -> str | None:
    for alias in (spec.target_name, *spec.aliases):
        canonical_alias = canonicalize_column_name(alias)
        if canonical_alias in canonical_to_original:
            return canonical_to_original[canonical_alias]
    return None
