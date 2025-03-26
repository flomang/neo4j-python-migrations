import tempfile
from pathlib import Path
from typing import List
from unittest.mock import Mock

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem

from neo4j_python_migrations import loader
from neo4j_python_migrations.migration import (
    CypherMigration,
    Migration,
    PythonMigration,
)


@pytest.mark.parametrize(
    "version, expected",
    [("0001", "0001"), ("1_1", "1.1"), ("51_1_10", "51.1.10")],
)
def test_prepare_version(version: str, expected: str) -> None:
    assert (
        loader._prepare_version(
            version,
        )
        == expected
    )


@pytest.mark.parametrize(
    "description, expected",
    [
        ("useless_migration", "useless migration"),
        ("nothing_special_1_", "nothing special 1"),
    ],
)
def test_prepare_name(description: str, expected: str) -> None:
    assert (
        loader._prepare_description(
            description,
        )
        == expected
    )


def test_load_cypher_migration(fs: FakeFilesystem) -> None:
    file_path = Path("./migrations/V0001__initial_migration.cypher")
    fs.create_file(file_path, contents="MATCH (n) RETURN n;")

    assert loader.load(file_path.parent) == [
        CypherMigration(
            version="0001",
            description="initial migration",
            source="V0001__initial_migration.cypher",
            query="MATCH (n) RETURN n;",
        ),
    ]


def test_load_python_migration() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        file_path = Path(tempdir).joinpath("V0001__initial_migration.py")
        with open(file_path, "w") as tmpfile:
            tmpfile.write(
                "def up(session): " "    session.test()",
            )

        migrations = loader.load(file_path.parent)

    session = Mock()
    migrations[0].code(session)  # type: ignore

    assert len(migrations) == 1
    assert isinstance(migrations[0], PythonMigration)
    assert Migration.from_other(migrations[0]) == Migration(
        version="0001",
        description="initial migration",
        source="V0001__initial_migration.py",
        type="PYTHON",
    )
    session.test.assert_called()


@pytest.mark.parametrize(
    "filenames, expected_versions",
    [
        (
            [
                "V0003__migration.cypher",
                "V0001__migration.cypher",
                "V0002__migration.cypher",
            ],
            ["0001", "0002", "0003"],
        ),
        (
            [
                "V0_3_0__migration.cypher",
                "V0_20_0__migration.cypher",
            ],
            ["0.3.0", "0.20.0"],
        ),
    ],
)
def test_migrations_order(
    fs: FakeFilesystem,
    filenames: List[str],
    expected_versions: List[str],
) -> None:
    migrations_path = Path("./migrations")
    migration_files = [migrations_path.joinpath(filename) for filename in filenames]
    for file_path in migration_files:
        fs.create_file(file_path, contents="MATCH (n) RETURN n;")

    loaded_migrations_versions = [
        migration.version for migration in loader.load(migrations_path)
    ]
    assert loaded_migrations_versions == expected_versions


def test_exception_on_two_identical_versions(fs: FakeFilesystem) -> None:
    migrations_path = Path("./migrations")
    migration_files = [
        migrations_path.joinpath("V100_1__some.cypher"),
        migrations_path.joinpath("V100_1__body.cypher"),
    ]
    for file_path in migration_files:
        fs.create_file(file_path, contents="MATCH (n) RETURN n;")

    with pytest.raises(ValueError):
        loader.load(migrations_path)


def test_no_migrations(fs: FakeFilesystem) -> None:
    migrations_path = Path("./migrations")
    fs.create_dir(migrations_path)

    assert not loader.load(migrations_path.parent)


def test_no_matching_files(fs: FakeFilesystem) -> None:
    migrations_path = Path("./migrations")
    migration_files = [
        migrations_path.joinpath("100_1__some.cypher"),
        migrations_path.joinpath("V100_1__body.cy"),
        migrations_path.joinpath("V0001__initial.java"),
        migrations_path.joinpath("V100_1_body.python"),
    ]
    for file_path in migration_files:
        fs.create_file(file_path)

    assert not loader.load(migrations_path.parent)


def test_load_python_migration_with_rollback() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        file_path = Path(tempdir).joinpath("V0001__migration_with_rollback.py")
        with open(file_path, "w") as tmpfile:
            tmpfile.write(
                "def up(session): \n"
                "    session.apply()\n"
                "\n"
                "def down(session): \n"
                "    session.rollback()"
            )

        migrations = loader.load(file_path.parent)

    session = Mock()
    
    # Test apply
    migrations[0].code(session)  # type: ignore
    session.apply.assert_called_once()
    
    # Test rollback
    migrations[0].rollback(session)  # type: ignore
    session.rollback.assert_called_once()
    
    assert len(migrations) == 1
    assert isinstance(migrations[0], PythonMigration)
    assert migrations[0].rollback_code is not None


def test_cypher_migration_with_up_down_sections(fs: FakeFilesystem) -> None:
    migration_content = """
    // ↑UP-MIGRATION
    CREATE (n:Test {name: 'test'});
    CREATE INDEX test_idx FOR (n:Test) ON (n.name);
    
    // ↓DOWN-MIGRATION
    DROP INDEX test_idx;
    MATCH (n:Test) DELETE n;
    """
    
    file_path = Path("./migrations/V0001__test_with_sections.cypher")
    fs.create_file(file_path, contents=migration_content)
    
    migrations = loader.load(file_path.parent)
    
    assert len(migrations) == 1
    migration = migrations[0]
    assert isinstance(migration, CypherMigration)
    assert len(migration.statements) == 2
    assert len(migration.rollback_statements) == 2
    assert "CREATE (n:Test {name: 'test'})" in migration.statements[0]
    assert "DROP INDEX test_idx" in migration.rollback_statements[0]
