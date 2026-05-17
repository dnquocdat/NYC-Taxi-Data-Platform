"""Basic package smoke tests."""

from nyc_taxi_pipeline import __version__


def test_package_version_is_defined() -> None:
    """The package exposes a version for logs and run metadata."""
    assert __version__
