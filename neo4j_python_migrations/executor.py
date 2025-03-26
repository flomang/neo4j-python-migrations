import time
from pathlib import Path
from typing import Callable, Optional

from neo4j import Driver

from neo4j_python_migrations import analyzer, loader
from neo4j_python_migrations.dao import MigrationDAO
from neo4j_python_migrations.migration import Migration


class Executor:
    """A class for working with migrations."""

    def __init__(  # noqa: WPS211
        self,
        driver: Driver,
        migrations_path: Path,
        project: Optional[str] = None,
        database: Optional[str] = None,
        schema_database: Optional[str] = None,
    ):
        """
        Initialize the class instance by loading local migrations from the file system.

        :param driver: Neo4j driver.
        :param migrations_path: the path to the directory containing migrations.
        :param project: the name of the project for differentiation migration
                        chains within the same database.
        :param database: the database that should be migrated (Neo4j EE).
        :param schema_database: the database that should be used for storing
                                information about migrations (Neo4j EE).
                                If not specified, then the database
                                that should be migrated is used.
        """
        if database and not schema_database:
            schema_database = database

        self.driver = driver
        self.dao = MigrationDAO(
            driver,
            project=project,
            database=database,
            schema_database=schema_database,
        )
        self.local_migrations = loader.load(migrations_path)
        self.database = database

    def migrate(  # noqa: WPS210
        self,
        version: Optional[str] = None,
        on_apply: Optional[Callable[[Migration], None]] = None,
    ) -> None:
        """
        Retrieves all pending migrations, verify and applies them.

        :param version: specific version to migrate to (inclusive).
                       If None, all pending migrations are applied.
        :param on_apply: callback that is called when each migration is applied.
        :raises ValueError: if errors were found during migration verification.
        """
        analyzing_result = self.analyze()
        if analyzing_result.invalid_versions:
            raise ValueError(
                "Errors were found during migration verification. "
                "Run the `analyze` command for more information.",
            )

        if not analyzing_result.latest_applied_version:
            self.dao.create_baseline()
            self.dao.create_constraints()

        # If version is specified, filter pending migrations up to that version (inclusive)
        migrations_to_apply = analyzing_result.pending_migrations
        if version is not None:
            # Find the index of the specified version in pending migrations
            version_index = None
            for i, migration in enumerate(migrations_to_apply):
                if migration.version == version:
                    version_index = i
                    break
            
            if version_index is None:
                raise ValueError(f"Migration version {version} not found in pending migrations.")
            
            # Apply migrations only up to the specified version (inclusive)
            migrations_to_apply = migrations_to_apply[:version_index + 1]
        
        for migration in migrations_to_apply:
            self.dao.add_migration(migration, 0, dry_run=True)

            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    start_time = time.monotonic()
                    migration.apply(tx)
                    duration = time.monotonic() - start_time

                    if on_apply:
                        on_apply(migration)  # noqa: WPS220

                self.dao.add_migration(migration, duration)
                
    def rollback(  # noqa: WPS210
        self,
        version: Optional[str] = None,
        on_rollback: Optional[Callable[[Migration], None]] = None,
    ) -> None:
        """
        Rollback the most recent migration or up to a specific version.

        :param version: specific version to rollback to (inclusive).
                        If None, only the most recent migration is rolled back.
        :param on_rollback: callback that is called when each migration is rolled back.
        :raises ValueError: if errors were found during migration verification or if
                          no migrations to rollback.
        """
        # Get applied migrations
        applied_migrations = self.dao.get_applied_migrations()
        
        if not applied_migrations:
            raise ValueError("No migrations found to rollback.")
        
        # Determine which migrations to rollback
        migrations_to_rollback = []
        
        if version is None:
            # Rollback only the most recent migration
            migrations_to_rollback = [applied_migrations[-1]]
        else:
            # Find the index of the specified version
            version_index = None
            for i, migration in enumerate(applied_migrations):
                if migration.version == version:
                    version_index = i
                    break
            
            if version_index is None:
                raise ValueError(f"Migration version {version} not found in applied migrations.")
            
            # Collect all migrations that need to be rolled back (in reverse order)
            migrations_to_rollback = list(reversed(applied_migrations[version_index + 1:]))
        
        # Rollback migrations in reverse order (newest first)
        for migration in migrations_to_rollback:
            # Find the local migration to get rollback information
            local_migration = next(
                (m for m in self.local_migrations if m.version == migration.version),
                None,
            )
            
            if local_migration is None:
                raise ValueError(
                    f"Local migration V{migration.version} not found. "
                    "Cannot perform rollback without local migration file.",
                )
            
            # Perform the rollback
            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    start_time = time.monotonic()
                    try:
                        local_migration.rollback(tx)
                        duration = time.monotonic() - start_time
                        
                        if on_rollback:
                            on_rollback(local_migration)
                            
                        # Remove the migration from the database
                        self.dao.remove_migration(migration.version)
                    except NotImplementedError as e:
                        raise ValueError(
                            f"Migration V{migration.version} does not support rollback: {str(e)}",
                        )

    def analyze(self) -> analyzer.AnalyzingResult:
        """
        Analyze local and remote migrations.

        Finds pending migrations and missed migrations.
        :return: analysis result.
        """
        applied_migrations = self.dao.get_applied_migrations()
        return analyzer.analyze(self.local_migrations, applied_migrations)
