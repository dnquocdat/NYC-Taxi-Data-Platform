"""Tests for Bronze ingestion manifest logic."""

from pathlib import Path

from nyc_taxi_pipeline.ingestion.manifest import (
    IngestionManifest,
    ManifestEntry,
    filter_pending_sources,
)
from nyc_taxi_pipeline.ingestion.source_discovery import SourceFile


def _source(month: int) -> SourceFile:
    return SourceFile(
        year=2023,
        month=month,
        file_name=f"yellow_tripdata_2023-{month:02d}.parquet",
        source_url=f"https://example.test/yellow_tripdata_2023-{month:02d}.parquet",
    )


def test_manifest_tracks_successful_source_urls(tmp_path: Path) -> None:
    """Successful manifest entries should be used for idempotent reruns."""
    manifest = IngestionManifest(tmp_path / "manifest.jsonl")
    source = _source(1)

    manifest.append(
        ManifestEntry(
            source_url=source.source_url,
            source_file=source.file_name,
            batch_id="batch-001",
            status="success",
            records_written=10,
        )
    )

    assert manifest.has_success(source.source_url) is True
    assert manifest.successful_source_urls() == {source.source_url}
    assert manifest.read_entries()[0].records_written == 10


def test_filter_pending_sources_skips_successful_sources() -> None:
    """Source discovery output should be filterable by manifest successes."""
    sources = [_source(1), _source(2)]

    pending = filter_pending_sources(sources, {sources[0].source_url})

    assert pending == [sources[1]]


def test_failed_entry_does_not_mark_source_successful(tmp_path: Path) -> None:
    """Failed attempts should not block future retries."""
    manifest = IngestionManifest(tmp_path / "manifest.jsonl")
    source = _source(1)

    manifest.append(
        ManifestEntry(
            source_url=source.source_url,
            source_file=source.file_name,
            batch_id="batch-001",
            status="failed",
            error_message="boom",
        )
    )

    assert manifest.has_success(source.source_url) is False
