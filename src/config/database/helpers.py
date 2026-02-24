"""Database configuration helper functions."""

import unicodedata

from src.config.database.concepts import CONCEPT_TO_TABLES
from src.config.database.schemas import DATABASE_TABLES
from src.config.database.types import ColumnInfo, TableInfo


def _strip_accents(text: str) -> str:
    """Remove diacritical marks (accents) from text."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def is_valid_table(table_name: str) -> bool:
    """Check if a table name is valid."""
    return table_name in DATABASE_TABLES


def is_valid_column(table_name: str, column_name: str) -> bool:
    """Check if a column name is valid for a table."""
    columns = get_table_columns(table_name)
    return any(col.column_name == column_name for col in columns)


def get_tables_for_query(query: str) -> set[str] | None:
    """Get all tables that are referenced in a query."""
    query_normalized = _strip_accents(query.lower())
    result = set()

    for concept, table_list in CONCEPT_TO_TABLES.items():
        if concept in query_normalized:
            result.update(table_list)

    return result if result else None


def get_table_info(table_name: str) -> TableInfo | None:
    """Get table information by table name."""
    return DATABASE_TABLES.get(table_name)


def get_all_table_names() -> list[str]:
    """Get all table names."""
    return list(DATABASE_TABLES.keys())


def get_table_columns(table_name: str) -> list[ColumnInfo]:
    """Get columns for a specific table."""
    table_info = get_table_info(table_name)
    if table_info:
        return table_info.table_columns
    return []


def get_column_info(table_name: str, column_name: str) -> ColumnInfo | None:
    """Get column information for a specific column in a table."""
    columns = get_table_columns(table_name)
    for col in columns:
        if col.column_name == column_name:
            return col
    return None
