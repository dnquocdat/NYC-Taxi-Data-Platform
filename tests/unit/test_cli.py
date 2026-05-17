"""CLI command registration tests."""

from click.testing import CliRunner

from nyc_taxi_pipeline.cli import main


def test_run_sample_command_is_registered() -> None:
    """The sample pipeline command is available for README smoke runs."""
    result = CliRunner().invoke(main, ["run-sample", "--help"])

    assert result.exit_code == 0
    assert "Run a tiny local Bronze to Silver sample path" in result.output
