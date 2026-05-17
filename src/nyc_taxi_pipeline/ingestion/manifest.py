"""Idempotent ingestion manifest helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol, TypeVar

ManifestStatus = Literal["success", "failed", "skipped"]


class HasSourceUrl(Protocol):
    """Protocol for source objects tracked by manifest URL."""

    source_url: str


SourceT = TypeVar("SourceT", bound=HasSourceUrl)


@dataclass(frozen=True)
class ManifestEntry:
    """One manifest event for a source-file ingestion attempt."""

    source_url: str
    source_file: str
    batch_id: str
    status: ManifestStatus
    ingested_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    records_written: int = 0
    error_message: str | None = None


class IngestionManifest:
    """JSONL-backed manifest used to make Bronze ingestion idempotent."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def read_entries(self) -> list[ManifestEntry]:
        """Read all manifest entries, returning an empty list when no manifest exists."""
        if not self.path.exists():
            return []

        entries: list[ManifestEntry] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            entries.append(ManifestEntry(**payload))
        return entries

    def append(self, entry: ManifestEntry) -> None:
        """Append one manifest entry."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(entry), ensure_ascii=False, sort_keys=True))
            handle.write("\n")

    def has_success(self, source_url: str) -> bool:
        """Return whether a source URL has already been ingested successfully."""
        return any(
            entry.source_url == source_url and entry.status == "success"
            for entry in self.read_entries()
        )

    def successful_source_urls(self) -> set[str]:
        """Return all source URLs with at least one successful ingestion entry."""
        return {entry.source_url for entry in self.read_entries() if entry.status == "success"}


def filter_pending_sources(
    sources: Iterable[SourceT],
    successful_source_urls: set[str],
) -> list[SourceT]:
    """Return source objects whose `source_url` is not marked successful."""
    return [source for source in sources if source.source_url not in successful_source_urls]
