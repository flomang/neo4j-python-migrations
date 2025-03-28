from typing import List
from unittest.mock import MagicMock, Mock, call

import pytest

from neo4j_python_migrations.migration import (
    CypherMigration,
    Migration,
    PythonMigration,
)


def test_apply_python_migration() -> None:
    code = Mock()
    migration = PythonMigration(
        version="0001",
        description="1234",
        code=code,
    )

    session = MagicMock()
    migration.apply(session)

    code.assert_called_with(session)


@pytest.mark.parametrize(
    "query, expected_checksum, expected_statements",
    [
        (
            (
                "MATCH (n) RETURN count(n) AS n;\n"
                "MATCH (n) RETURN count(n) AS n;\n"
                "MATCH (n) RETURN count(n) AS n;\n"
            ),
            "1902097523",
            [
                "MATCH (n) RETURN count(n) AS n",
                "MATCH (n) RETURN count(n) AS n",
                "MATCH (n) RETURN count(n) AS n",
            ],
        ),
        (
            "//some comment\n"
            "  MATCH (n) RETURN n;\n"
            "\n"
            " //some other comment\n"
            " MATCH (n)\n"
            "    RETURN (n);\n"
            ";\n",
            "3156131171",
            [
                "//some comment\n  MATCH (n) RETURN n",
                "//some other comment\n MATCH (n)\n    RETURN (n)",
            ],
        ),
    ],
)
def test_init_cypher_migration(
    query: str,
    expected_checksum: str,
    expected_statements: List[str],
) -> None:
    migration = CypherMigration(
        version="0001",
        description="1234",
        query=query,
    )

    assert migration.checksum == expected_checksum
    assert migration.statements == expected_statements


def test_apply_cypher_migration() -> None:
    migration = CypherMigration(
        version="0001",
        description="1234",
        query="STATEMENT1;STATEMENT2;",
    )

    session = MagicMock()
    migration.apply(session)

    assert call.run("STATEMENT1") in session.mock_calls
    assert call.run("STATEMENT2") in session.mock_calls


def test_migration_from_child() -> None:
    child = PythonMigration(
        version="0001",
        description="initial",
        source="V0001__initial.py",
        code=Mock(),
    )
    parent = Migration.from_other(child)
    assert parent == Migration(
        version="0001",
        description="initial",
        source="V0001__initial.py",
        type="PYTHON",
    )


def test_migration_from_dict() -> None:
    db_properties = {
        "version": "0001",
        "description": "initial",
        "source": "V0001__initial.cypher",
        "type": "CYPHER",
    }
    migration = Migration.from_dict(db_properties)
    assert migration == Migration(
        version="0001",
        description="initial",
        source="V0001__initial.cypher",
        type="CYPHER",
    )


def test_migration_from_other() -> None:
    child_migration = PythonMigration(
        version="0001",
        description="initial",
        source="V0001__initial.py",
        code=Mock(),
    )
    migration = Migration.from_other(child_migration)
    assert migration == Migration(
        version="0001",
        description="initial",
        source="V0001__initial.py",
        type="PYTHON",
    )


def test_exception_if_apply_method_is_not_implemented() -> None:
    migration = Migration(version="0001", description="initial", type="CYPHER")

    with pytest.raises(NotImplementedError):
        migration.apply(Mock())


def test_exception_if_rollback_method_is_not_implemented() -> None:
    migration = Migration(version="0001", description="initial", type="CYPHER")

    with pytest.raises(NotImplementedError):
        migration.rollback(Mock())


def test_rollback_python_migration() -> None:
    code = Mock()
    rollback_code = Mock()
    migration = PythonMigration(
        version="0001",
        description="1234",
        code=code,
        rollback_code=rollback_code,
    )

    session = MagicMock()
    migration.rollback(session)

    rollback_code.assert_called_with(session)


def test_exception_if_python_rollback_not_implemented() -> None:
    code = Mock()
    migration = PythonMigration(
        version="0001",
        description="1234",
        code=code,
        # No rollback_code provided
    )

    with pytest.raises(NotImplementedError):
        migration.rollback(Mock())


@pytest.mark.parametrize(
    "query, expected_forward_statements, expected_down_statements",
    [
        # Case 1: No sections - treat all as forward
        (
            "MATCH (n) RETURN count(n) AS n;\nMATCH (m) RETURN count(m) AS m;",
            ["MATCH (n) RETURN count(n) AS n", "MATCH (m) RETURN count(m) AS m"],
            [],
        ),
        # Case 2: With FORWARD and DOWN sections
        (
            "↑UP-MIGRATION\nCREATE (n:Test);\nMATCH (n:Test) RETURN n;\n// ↓DOWN-MIGRATION\nMATCH (n:Test) DELETE n;",
            ["CREATE (n:Test)", "MATCH (n:Test) RETURN n"],
            ["MATCH (n:Test) DELETE n"],
        ),
        # Case 3: With empty DOWN section
        (
            "↑UP-MIGRATION\nCREATE (n:Test);\n// ↓DOWN-MIGRATION",
            ["CREATE (n:Test)"],
            [],
        ),
        # Case 4: With multiple statements in both sections
        (
            "↑UP-MIGRATION\nCREATE (n:Test);\nCREATE (m:Test2);\n// ↓DOWN-MIGRATION\nMATCH (n:Test) DELETE n;\nMATCH (m:Test2) DELETE m;",
            ["CREATE (n:Test)", "CREATE (m:Test2)"],
            ["MATCH (n:Test) DELETE n", "MATCH (m:Test2) DELETE m"],
        ),
    ],
)
def test_parse_sections_in_cypher_migration(
    query: str,
    expected_forward_statements: List[str],
    expected_down_statements: List[str],
) -> None:
    migration = CypherMigration(
        version="0001",
        description="1234",
        query=query,
    )

    assert migration.statements == expected_forward_statements
    assert migration.rollback_statements == expected_down_statements


def test_apply_and_rollback_cypher_migration() -> None:
    migration = CypherMigration(
        version="0001",
        description="1234",
        query="↑UP-MIGRATION\nSTATEMENT1;STATEMENT2;\n// ↓DOWN-MIGRATION\nROLLBACK1;ROLLBACK2;",
    )

    # Test apply
    session = MagicMock()
    migration.apply(session)

    assert call.run("STATEMENT1") in session.mock_calls
    assert call.run("STATEMENT2") in session.mock_calls
    session.reset_mock()
    
    # Test rollback
    migration.rollback(session)
    
    assert call.run("ROLLBACK1") in session.mock_calls
    assert call.run("ROLLBACK2") in session.mock_calls


def test_exception_if_cypher_rollback_not_implemented() -> None:
    migration = CypherMigration(
        version="0001",
        description="1234",
        query="STATEMENT1;STATEMENT2;",  # No DOWN section
    )

    with pytest.raises(NotImplementedError):
        migration.rollback(Mock())
