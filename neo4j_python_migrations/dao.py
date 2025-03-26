from functools import cached_property
from getpass import getuser
from typing import List, Optional

from neo4j import Driver

from neo4j_python_migrations.migration import Migration


class MigrationDAO:
    """DAO for working with the migration schema."""

    def __init__(
        self,
        driver: Driver,
        project: Optional[str] = None,
        database: Optional[str] = None,
        schema_database: Optional[str] = None,
    ):
        self.driver = driver
        self.project = project
        self.schema_database = schema_database
        self.database = None if database == schema_database else database
        self.baseline = "BASELINE"

    @cached_property
    def user(self) -> Optional[str]:
        """
        The name of the user connected to the database.

        :returns: the name.
        """
        with self.driver.session(database=self.schema_database) as session:
            query_result = session.run("SHOW CURRENT USER").single()
            if query_result:
                return query_result.value("user")
            return None

    def create_baseline(self) -> None:
        """Create a base node if it doesn't already exist."""
        with self.driver.session(database=self.schema_database) as session:
            query_params = {
                "version": self.baseline,
                "project": self.project,
                "migration_target": self.database,
            }
            query_result = session.run(
                """
                MATCH (m:__Neo4jMigration {version: $version})
                WHERE
                    coalesce(m.project,'<default>')
                        = coalesce($project,'<default>')
                    AND coalesce(m.migrationTarget,'<default>')
                        = coalesce($migration_target,'<default>')
                RETURN m
                """,
                query_params,
            )
            if query_result.single():
                return

            session.run(
                """
                CREATE (:__Neo4jMigration {
                    version: $version,
                    project: $project,
                    migrationTarget: $migration_target
                })
                """,
                query_params,
            )

    def create_constraints(self) -> None:
        """
        Create constraints in the database.

        This is useful for maintaining the integrity of the migration schema.
        """
        with self.driver.session(database=self.schema_database) as session:
            session.run(
                """
                CREATE CONSTRAINT unique_version___Neo4jMigration
                IF NOT EXISTS FOR (m:__Neo4jMigration)
                REQUIRE (m.version, m.project, m.migrationTarget) IS UNIQUE
                """,
            )

    def add_migration(
        self,
        migration: Migration,
        duration: float,
        dry_run: bool = False,
    ) -> None:
        """
        Add a migration record.

        :param migration: applied migration.
        :param duration: duration of migration execution (seconds).
        :param dry_run: do not make actual changes.
        :raises ValueError: if the migration record has not been created.
        """
        with self.driver.session(database=self.schema_database) as session:
            with session.begin_transaction() as tx:
                run_result = tx.run(
                    """
                    MATCH (m1:__Neo4jMigration)
                    WHERE
                        coalesce(m1.project,'<default>')
                            = coalesce($project,'<default>')
                        AND coalesce(m1.migrationTarget,'<default>')
                            = coalesce($migration_target,'<default>')
                        AND NOT (m1)-[:MIGRATED_TO]->(:__Neo4jMigration)
                    WITH m1
                    CREATE (m2:__Neo4jMigration {
                            version: $version_to,
                            description: $description,
                            type: $type,
                            source: $source,
                            project: $project,
                            migrationTarget: $migration_target,
                            checksum: $checksum
                        }
                    )
                    MERGE (m1)-[link:MIGRATED_TO]->(m2)
                    SET
                        link.at = datetime(),
                        link.in = duration({seconds: $duration}),
                        link.by = $migrated_by,
                        link.connectedAs = $connected_as
                    """,
                    version_to=migration.version,
                    description=migration.description,
                    source=migration.source,
                    type=migration.type,
                    checksum=migration.checksum,
                    duration=duration,
                    project=self.project,
                    migration_target=self.database,
                    migrated_by=getuser(),
                    connected_as=self.user,
                )
                result_summary = run_result.consume()
                if dry_run:
                    tx.rollback()
                if (
                    result_summary.counters.nodes_created != 1
                    and result_summary.counters.relationships_created != 1
                ):
                    raise ValueError(
                        "The migration record could not be created. "
                        "Check the migration graph.",
                    )

    def get_applied_migrations(self) -> List[Migration]:
        """
        Get an ordered list of applied migrations to the database.

        The Baseline is ignored.
        :return: sorted list of migrations.
        """
        with self.driver.session(database=self.schema_database) as session:
            query_result = session.run(
                """
                MATCH (:__Neo4jMigration{
                        version: $baseline
                })-[:MIGRATED_TO*]->(m:__Neo4jMigration)
                WHERE
                    coalesce(m.project,'<default>')
                        = coalesce($project,'<default>')
                    AND coalesce(m.migrationTarget,'<default>')
                        = coalesce($migration_target,'<default>')
                WITH m,
                    [x IN split(m.version, '.') | toInteger(x)] AS version
                RETURN m
                ORDER BY version
                """,
                baseline=self.baseline,
                project=self.project,
                migration_target=self.database,
            )
            return [Migration.from_dict(row.data()["m"]) for row in query_result]
            
    def remove_migration(self, version: str) -> None:
        """
        Remove a migration record from the database during rollback.
        
        This method detaches the migration node from the chain and deletes it.
        It also reconnects the previous migration node with the next one (if exists).
        
        :param version: The version of the migration to remove.
        :raises ValueError: If the migration could not be removed.
        """
        with self.driver.session(database=self.schema_database) as session:
            with session.begin_transaction() as tx:
                # Get the migration node, its predecessor, and its successor (if any)
                query_result = tx.run(
                    """
                    MATCH (prev:__Neo4jMigration)-[r1:MIGRATED_TO]->(m:__Neo4jMigration {version: $version})
                    WHERE
                        coalesce(m.project,'<default>') = coalesce($project,'<default>')
                        AND coalesce(m.migrationTarget,'<default>') = coalesce($migration_target,'<default>')
                    OPTIONAL MATCH (m)-[r2:MIGRATED_TO]->(next:__Neo4jMigration)
                    RETURN prev, m, next, r1, r2
                    """,
                    version=version,
                    project=self.project,
                    migration_target=self.database,
                )
                
                result = query_result.single()
                if not result:
                    raise ValueError(f"Migration version {version} not found.")
                
                # If there's a next migration, create a new relationship from prev to next
                if result.get("next"):
                    tx.run(
                        """
                        MATCH (prev:__Neo4jMigration)-[:MIGRATED_TO]->(m:__Neo4jMigration {version: $version})-[:MIGRATED_TO]->(next:__Neo4jMigration)
                        WHERE
                            coalesce(m.project,'<default>') = coalesce($project,'<default>')
                            AND coalesce(m.migrationTarget,'<default>') = coalesce($migration_target,'<default>')
                        MERGE (prev)-[new_link:MIGRATED_TO]->(next)
                        SET
                            new_link.at = datetime(),
                            new_link.by = $rolled_back_by,
                            new_link.connectedAs = $connected_as
                        """,
                        version=version,
                        project=self.project,
                        migration_target=self.database,
                        rolled_back_by=getuser(),
                        connected_as=self.user,
                    )
                
                # Delete the migration node and its relationships
                delete_result = tx.run(
                    """
                    MATCH (prev:__Neo4jMigration)-[r1:MIGRATED_TO]->(m:__Neo4jMigration {version: $version})
                    WHERE
                        coalesce(m.project,'<default>') = coalesce($project,'<default>')
                        AND coalesce(m.migrationTarget,'<default>') = coalesce($migration_target,'<default>')
                    OPTIONAL MATCH (m)-[r2:MIGRATED_TO]->(next:__Neo4jMigration)
                    DELETE r1, r2, m
                    RETURN count(m) as deleted_count
                    """,
                    version=version,
                    project=self.project,
                    migration_target=self.database,
                )
                
                summary = delete_result.single()
                if not summary or summary["deleted_count"] != 1:
                    raise ValueError(f"Failed to remove migration version {version}.")
