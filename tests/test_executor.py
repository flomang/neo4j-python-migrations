from typing import Optional
from unittest.mock import MagicMock, Mock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch
from neo4j import Driver

from neo4j_python_migrations import dao
from neo4j_python_migrations.analyzer import (
    AnalyzingResult,
    InvalidVersion,
    InvalidVersionStatus,
)
from neo4j_python_migrations.executor import Executor
from neo4j_python_migrations.migration import CypherMigration, Migration
from tests.conftest import can_connect_to_neo4j


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.executor.Executor.analyze")
def test_migrate_when_there_are_no_remote_migrations(
    executor_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:
    migration = Mock()
    executor_mock.return_value = AnalyzingResult(pending_migrations=[migration])
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    executor.migrate()

    migration.apply.assert_called()
    executor.dao.create_baseline.assert_called()
    executor.dao.add_migration.assert_called()


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.executor.Executor.analyze")
def test_migrate_with_on_apply_callback(
    executor_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:
    migration = Mock()
    on_apply = Mock()
    executor_mock.return_value = AnalyzingResult(pending_migrations=[migration])
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    executor.migrate(on_apply=on_apply)

    migration.apply.assert_called()
    executor.dao.create_baseline.assert_called()
    executor.dao.add_migration.assert_called()
    on_apply.assert_called_with(migration)


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.executor.Executor.analyze")
def test_migrate_when_there_are_remote_migrations(
    executor_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:
    migration = Mock()
    executor_mock.return_value = AnalyzingResult(pending_migrations=[migration])

    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    executor.migrate()

    migration.apply.assert_called()
    executor.dao.create_baseline.assert_called()
    executor.dao.add_migration.assert_called()


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.executor.Executor.analyze")
def test_migrate_when_are_invalid_versions(
    executor_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:

    executor_mock.return_value = AnalyzingResult(
        invalid_versions=[
            InvalidVersion("0001", InvalidVersionStatus.DIFFERENT),
        ],
    )
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    with pytest.raises(ValueError):
        executor.migrate()


@patch("neo4j_python_migrations.loader.load")
@pytest.mark.parametrize(
    "db, schema_db, expected_db",
    [
        (None, None, None),
        ("test", None, "test"),
        ("test1", "test2", "test2"),
        (None, "test2", "test2"),
    ],
)
def test_dao_schema_database(
    loader_mock: MagicMock,
    db: Optional[str],
    schema_db: Optional[str],
    expected_db: Optional[str],
) -> None:
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
        database=db,
        schema_database=schema_db,
    )
    assert executor.dao.schema_database == expected_db


@pytest.mark.skipif(not can_connect_to_neo4j(), reason="Can't connect to Neo4j")
@patch("neo4j_python_migrations.loader.load")
def test_dao_errors_cause_rollback(
    loader_mock: MagicMock,
    neo4j_driver: Driver,
    monkeypatch: MonkeyPatch,
) -> None:
    migration = CypherMigration(
        version="0001",
        description="123",
        query="CREATE CONSTRAINT foobar FOR (n:Test) REQUIRE n.id IS UNIQUE;",
    )
    executor = Executor(
        driver=neo4j_driver,
        migrations_path=Mock(),
    )

    def getuser() -> None:
        raise Exception("Test exception")

    monkeypatch.setattr(dao, "getuser", getuser)

    executor.analyze = Mock()  # type: ignore
    executor.analyze.return_value = AnalyzingResult(pending_migrations=[migration])
    with pytest.raises(Exception, match="Test exception"):
        executor.migrate()

    with neo4j_driver.session() as session:
        x = session.run("SHOW CONSTRAINTS YIELD name")
        names = [i[0] for i in x]
        assert "foobar" not in names


@pytest.mark.skipif(not can_connect_to_neo4j(), reason="Can't connect to Neo4j")
@patch("neo4j_python_migrations.loader.load")
def test_on_apply_errors_cause_rollback(
    loader_mock: MagicMock,
    neo4j_driver: Driver,
) -> None:
    migration = CypherMigration(
        version="0001",
        description="123",
        query="CREATE CONSTRAINT foobar FOR (n:Test) REQUIRE n.id IS UNIQUE;",
    )
    executor = Executor(
        driver=neo4j_driver,
        migrations_path=Mock(),
    )

    executor.analyze = Mock()  # type: ignore
    executor.analyze.return_value = AnalyzingResult(pending_migrations=[migration])

    def on_apply(migration: Migration) -> None:
        raise Exception("Test exception")

    with pytest.raises(Exception, match="Test exception"):
        executor.migrate(on_apply=on_apply)

    with neo4j_driver.session() as session:
        x = session.run("SHOW CONSTRAINTS YIELD name")
        names = [i[0] for i in x]
        assert "foobar" not in names


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.dao.MigrationDAO.get_applied_migrations")
def test_rollback_single_migration(
    get_applied_migrations_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:
    # Setup mocks with actual list for applied migrations
    applied_migration = Migration(version="0002", description="test", type="CYPHER")
    applied_migrations = [applied_migration]
    get_applied_migrations_mock.return_value = applied_migrations
    
    # Create a local migration with rollback support
    local_migration = Mock()
    local_migration.version = "0002"
    loader_mock.return_value = [local_migration]
    
    # Setup executor
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    executor.dao.get_applied_migrations.return_value = applied_migrations
    
    # Execute rollback
    on_rollback = Mock()
    executor.rollback(on_rollback=on_rollback)
    
    # Verify expectations
    local_migration.rollback.assert_called_once()
    executor.dao.remove_migration.assert_called_once_with("0002")
    on_rollback.assert_called_once_with(local_migration)


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.dao.MigrationDAO.get_applied_migrations")
def test_rollback_to_specific_version(
    get_applied_migrations_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:
    # Setup mocks for applied migrations with actual list
    applied_migrations = [
        Migration(version="0001", description="first", type="CYPHER"),
        Migration(version="0002", description="second", type="CYPHER"),
        Migration(version="0003", description="third", type="CYPHER"),
    ]
    get_applied_migrations_mock.return_value = applied_migrations
    
    # Create local migrations with rollback support
    local_migrations = [
        Mock(version="0001"),
        Mock(version="0002"),
        Mock(version="0003"),
    ]
    loader_mock.return_value = local_migrations
    
    # Setup executor
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    executor.dao.get_applied_migrations.return_value = applied_migrations
    
    # Execute rollback to version 0001 (will rollback 0003 and 0002)
    executor.rollback(version="0001")
    
    # Verify expectations - should rollback in reverse order
    local_migrations[2].rollback.assert_called_once()
    local_migrations[1].rollback.assert_called_once()
    local_migrations[0].rollback.assert_not_called()  # Should not rollback 0001
    
    # Should remove migrations in the correct order
    assert executor.dao.remove_migration.call_args_list[0][0][0] == "0003"
    assert executor.dao.remove_migration.call_args_list[1][0][0] == "0002"
    assert len(executor.dao.remove_migration.call_args_list) == 2


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.dao.MigrationDAO.get_applied_migrations")
def test_rollback_with_missing_local_migration(
    get_applied_migrations_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:
    # Setup mocks with actual list
    applied_migration = Migration(version="0002", description="test", type="CYPHER")
    applied_migrations = [applied_migration]
    get_applied_migrations_mock.return_value = applied_migrations
    
    # No matching local migration
    loader_mock.return_value = []
    
    # Setup executor
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    executor.dao.get_applied_migrations.return_value = applied_migrations
    
    # Should raise ValueError when local migration is not found
    with pytest.raises(ValueError, match="Local migration V0002 not found"):
        executor.rollback()


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.dao.MigrationDAO.get_applied_migrations")
def test_rollback_with_non_implemented_rollback(
    get_applied_migrations_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:
    # Setup mocks with actual list
    applied_migration = Migration(version="0002", description="test", type="CYPHER")
    applied_migrations = [applied_migration]
    get_applied_migrations_mock.return_value = applied_migrations
    
    # Local migration that will raise NotImplementedError on rollback
    local_migration = Mock()
    local_migration.version = "0002"
    local_migration.rollback.side_effect = NotImplementedError("Rollback not implemented")
    loader_mock.return_value = [local_migration]
    
    # Setup executor
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    executor.dao.get_applied_migrations.return_value = applied_migrations
    
    # Should raise ValueError when rollback is not implemented
    with pytest.raises(ValueError, match="does not support rollback"):
        executor.rollback()


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.dao.MigrationDAO.get_applied_migrations")
def test_rollback_no_migrations_to_rollback(
    get_applied_migrations_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:
    # No applied migrations - empty list instead of mock
    applied_migrations = []
    get_applied_migrations_mock.return_value = applied_migrations
    
    # Setup executor
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    executor.dao.get_applied_migrations.return_value = applied_migrations
    
    # Should raise ValueError when no migrations exist
    with pytest.raises(ValueError, match="No migrations found to rollback"):
        executor.rollback()


@patch("neo4j_python_migrations.loader.load")
@patch("neo4j_python_migrations.dao.MigrationDAO.get_applied_migrations")
def test_rollback_invalid_version(
    get_applied_migrations_mock: MagicMock,
    loader_mock: MagicMock,
) -> None:
    # Setup applied migrations with actual list
    applied_migrations = [
        Migration(version="0001", description="first", type="CYPHER"),
        Migration(version="0002", description="second", type="CYPHER"),
    ]
    get_applied_migrations_mock.return_value = applied_migrations
    
    # Create local migrations
    local_migrations = [
        Mock(version="0001"),
        Mock(version="0002"),
    ]
    loader_mock.return_value = local_migrations
    
    # Setup executor
    executor = Executor(
        driver=MagicMock(),
        migrations_path=Mock(),
    )
    executor.dao = Mock()
    executor.dao.get_applied_migrations.return_value = applied_migrations
    
    # Should raise ValueError when specified version doesn't exist
    with pytest.raises(ValueError, match="Migration version 0003 not found"):
        executor.rollback(version="0003")
