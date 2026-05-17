"""Command-line entry points for local pipeline operations."""

from __future__ import annotations

import click


@click.group()
def main() -> None:
    """Run NYC taxi pipeline commands."""


@main.command("run-sample")
def run_sample() -> None:
    """Run the small sample pipeline path once it is implemented."""
    raise click.ClickException(
        "Sample pipeline is not implemented yet. It will be added in Phase 5-6."
    )


if __name__ == "__main__":
    main()
