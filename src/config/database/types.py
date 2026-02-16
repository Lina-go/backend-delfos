"""Database type definitions."""

from dataclasses import dataclass

from src.config.constants import ColumnType


@dataclass(frozen=True)
class ColumnInfo:
    """Information about a database column."""

    column_name: str
    column_type: ColumnType
    column_description: str


@dataclass(frozen=True)
class TableInfo:
    """Information about a database table."""

    table_name: str
    table_description: str
    table_columns: list[ColumnInfo]
