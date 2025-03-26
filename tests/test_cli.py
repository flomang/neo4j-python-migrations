from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from neo4j_python_migrations.analyzer import (
    AnalyzingResult,
    InvalidVersion,
    InvalidVersionStatus,
)
from neo4j_python_migrations.cli import cli
from neo4j_python_migrations.migration import Migration

runner = CliRunner()


@patch("neo4j.GraphDatabase.driver")
def test_analyze_when_there_are_pending_migrations(driver: MagicMock) -> None:
    with patch("neo4j_python_migrations.executor.Executor.analyze") as executor_mock:
        executor_mock.return_value = AnalyzingResult(
            pending_migrations=[
                Migration(version="0001", description="", type="CYPHER"),
            ],
        )
        result = runner.invoke(cli, ["--path", ".", "--password", "test", "analyze"])

    assert result.exit_code == 0


@patch("neo4j.GraphDatabase.driver")
def test_analyze_when_there_are_no_pending_migrations(driver: MagicMock) -> None:
    with patch("neo4j_python_migrations.executor.Executor.analyze") as executor_mock:
        executor_mock.return_value = AnalyzingResult()
        result = runner.invoke(cli, ["--path", ".", "--password", "test", "analyze"])
    assert result.exit_code == 0


@patch("neo4j.GraphDatabase.driver")
def test_analyze_when_there_are_invalid_versions(driver: MagicMock) -> None:
    with patch("neo4j_python_migrations.executor.Executor.analyze") as executor_mock:
        executor_mock.return_value = AnalyzingResult(
            invalid_versions=[
                InvalidVersion("0001", InvalidVersionStatus.MISSED_LOCALLY),
            ],
        )
        result = runner.invoke(cli, ["--path", ".", "--password", "test", "analyze"])

    assert result.exit_code != 0


@patch("neo4j.GraphDatabase.driver")
def test_migrate_default(driver: MagicMock) -> None:
    with patch("neo4j_python_migrations.executor.Executor.migrate") as executor_mock:
        result = runner.invoke(cli, ["--path", ".", "--password", "test", "migrate"])

        assert result.exit_code == 0
        # Should be called with version=None
        assert executor_mock.call_args[1]["version"] is None
        assert "on_apply" in executor_mock.call_args[1]


@patch("neo4j.GraphDatabase.driver")
def test_migrate_to_specific_version(driver: MagicMock) -> None:
    with patch("neo4j_python_migrations.executor.Executor.migrate") as executor_mock:
        # The version is a positional argument, not an option
        result = runner.invoke(cli, ["--path", ".", "--password", "test", "migrate", "0001"])

        assert result.exit_code == 0
        # Check version was passed correctly
        assert executor_mock.call_args[1]["version"] == "0001"
        assert "on_apply" in executor_mock.call_args[1]


@patch("neo4j.GraphDatabase.driver")
def test_migrate_error_handling(driver: MagicMock) -> None:
    with patch("neo4j_python_migrations.executor.Executor.migrate") as executor_mock:
        executor_mock.side_effect = ValueError("Test migration error")
        result = runner.invoke(cli, ["--path", ".", "--password", "test", "migrate"])

        # CLI returns exit code 1 on error
        assert result.exit_code == 1
        assert "Test migration error" in result.stdout


@patch("neo4j.GraphDatabase.driver")
def test_rollback_default(driver: MagicMock) -> None:
    with patch("neo4j_python_migrations.executor.Executor.rollback") as executor_mock:
        result = runner.invoke(cli, ["--path", ".", "--password", "test", "rollback"])

        assert result.exit_code == 0
        # We can't directly assert the lambda function equality, so just check it was called with version=None
        assert executor_mock.call_args[1]["version"] is None
        assert "on_rollback" in executor_mock.call_args[1]


@patch("neo4j.GraphDatabase.driver")
def test_rollback_to_specific_version(driver: MagicMock) -> None:
    with patch("neo4j_python_migrations.executor.Executor.rollback") as executor_mock:
        # The version is a positional argument, not an option
        result = runner.invoke(cli, ["--path", ".", "--password", "test", "rollback", "0001"])

        assert result.exit_code == 0
        # Check version was passed correctly
        assert executor_mock.call_args[1]["version"] == "0001"
        assert "on_rollback" in executor_mock.call_args[1]


@patch("neo4j.GraphDatabase.driver")
def test_rollback_error_handling(driver: MagicMock) -> None:
    with patch("neo4j_python_migrations.executor.Executor.rollback") as executor_mock:
        executor_mock.side_effect = ValueError("Test rollback error")
        result = runner.invoke(cli, ["--path", ".", "--password", "test", "rollback"])

        # CLI returns exit code 1 on error
        assert result.exit_code == 1
        assert "Test rollback error" in result.stdout
