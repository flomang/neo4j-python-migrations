import binascii
import re
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from attr import asdict, define, field
from neo4j import Transaction
from packaging.version import Version


class MigrationType(str, Enum):  # noqa: WPS600
    """The type of migration to store in the database."""

    PYTHON = "PYTHON"
    CYPHER = "CYPHER"


@define(kw_only=True, order=False)
class Migration:
    """The base class for all migrations."""

    version: str
    parsed_version: Version = field(init=False)
    description: str
    type: str
    source: Optional[str] = None
    checksum: Optional[str] = None
    rollback_checksum: Optional[str] = None

    @classmethod
    def from_dict(cls, properties: Dict[str, Any]) -> "Migration":
        """
        Get a class instance from a dictionary.

        :param properties: the dictionary.
        :return: class instance.
        """
        return Migration(
            version=properties["version"],
            description=properties["description"],
            type=properties["type"],
            source=properties.get("source"),
            checksum=properties.get("checksum"),
        )

    @classmethod
    def from_other(cls, other: Any) -> "Migration":
        """
        Get a class instance of a base class from a child.

        :param other: the child.
        :return: class instance.
        """
        return cls.from_dict(asdict(other))

    def apply(self, tx: Transaction) -> None:
        """
        Apply migration to the database.

        :param tx: neo4j transaction.
        :raises NotImplementedError: if not implemented.
        """
        raise NotImplementedError()
        
    def rollback(self, tx: Transaction) -> None:
        """
        Rollback migration from the database.
        
        :param tx: neo4j transaction.
        :raises NotImplementedError: if not implemented.
        """
        raise NotImplementedError()

    def __attrs_post_init__(self) -> None:
        self.parsed_version = Version(self.version)  # noqa: WPS601

    def __lt__(self, other: Any) -> bool:
        return self.parsed_version < other.parsed_version


@define
class PythonMigration(Migration):
    """Migration based on a python code."""

    code: Callable[[Transaction], None]
    rollback_code: Optional[Callable[[Transaction], None]] = None
    type: str = field(default=MigrationType.PYTHON, init=False)

    def apply(self, tx: Transaction) -> None:  # noqa: D102
        self.code(tx)
        
    def rollback(self, tx: Transaction) -> None:  # noqa: D102
        if self.rollback_code is None:
            raise NotImplementedError(f"Rollback not implemented for migration V{self.version}")
        self.rollback_code(tx)


@define
class CypherMigration(Migration):
    """Migration based on a cypher script."""

    query: str = field(repr=False)
    type: str = field(default=MigrationType.CYPHER, init=False)
    statements: List[str] = field(init=False, repr=False)
    rollback_statements: List[str] = field(init=False, repr=False, default=[])

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        forward_statements, down_statements = self._parse_statements(self.query)
        
        self.statements = list(  # noqa: WPS601
            filter(
                lambda statement: statement,
                [statement.strip() for statement in forward_statements],
            ),
        )
        
        self.rollback_statements = list(
            filter(
                lambda statement: statement,
                [statement.strip() for statement in down_statements],
            ),
        )

        # Calculate checksum for forward statements
        checksum = None
        for st in self.statements:
            binary_statement = st.encode()
            checksum = (
                binascii.crc32(binary_statement, checksum)  # type: ignore
                if checksum
                else binascii.crc32(binary_statement)
            )

        self.checksum = str(checksum) if checksum else None
        
        # Calculate checksum for rollback statements
        rollback_checksum = None
        for st in self.rollback_statements:
            binary_statement = st.encode()
            rollback_checksum = (
                binascii.crc32(binary_statement, rollback_checksum)  # type: ignore
                if rollback_checksum
                else binascii.crc32(binary_statement)
            )

        self.rollback_checksum = str(rollback_checksum) if rollback_checksum else None
        
    def _parse_statements(self, query: str) -> Tuple[List[str], List[str]]:
        """
        Parse the query into forward and down statements.
        
        :param query: The full query string.
        :return: A tuple of (forward_statements, down_statements).
        """
        if '↑UP-MIGRATION' not in query and '// ↓DOWN-MIGRATION' not in query:
            # If no sections are defined, treat the entire script as forward migration
            return (query.split(";")[:-1], [])
            
        # Split the query into sections
        forward_pattern = re.compile(r'↑UP-MIGRATION\s*(.*?)(?=// ↓DOWN-MIGRATION|$)', re.DOTALL)
        down_pattern = re.compile(r'// ↓DOWN-MIGRATION\s*(.*?)(?=$)', re.DOTALL)
        
        forward_match = forward_pattern.search(query)
        down_match = down_pattern.search(query)
        
        forward_content = forward_match.group(1).strip() if forward_match else ""
        down_content = down_match.group(1).strip() if down_match else ""
        
        forward_statements = forward_content.split(";") if forward_content else []
        if forward_statements and forward_statements[-1].strip() == "":
            forward_statements = forward_statements[:-1]
            
        down_statements = down_content.split(";") if down_content else []
        if down_statements and down_statements[-1].strip() == "":
            down_statements = down_statements[:-1]
        
        return (forward_statements, down_statements)

    def apply(self, tx: Transaction) -> None:  # noqa: D102
        for statement in self.statements:
            tx.run(statement)
            
    def rollback(self, tx: Transaction) -> None:  # noqa: D102
        if not self.rollback_statements:
            raise NotImplementedError(f"Rollback not implemented for migration V{self.version}")
        
        for statement in self.rollback_statements:
            tx.run(statement)
