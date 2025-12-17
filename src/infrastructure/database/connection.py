"""Direct database connection utilities using pyodbc."""

import asyncio
import logging
from typing import Any

import pyodbc

from src.config.settings import Settings

logger = logging.getLogger(__name__)


async def execute_query(
    settings: Settings, sql: str, params: tuple[Any, ...] | None = None
) -> list[dict[str, Any]]:
    """
    Execute a SELECT query and return results as a list of dictionaries.

    Args:
        settings: Application settings containing database_connection_string
        sql: SQL query string (use ? placeholders for parameters)
        params: Optional tuple of parameters for parameterized queries

    Returns:
        List of dictionaries, where each dictionary represents a row with column names as keys

    Raises:
        Exception: If database connection or query execution fails
    """
    if not settings.database_connection_string:
        raise ValueError("database_connection_string is not configured in settings")

    def _execute() -> list[dict[str, Any]]:
        """Execute query synchronously in thread pool."""
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(settings.database_connection_string)
            cursor = conn.cursor()

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            # Get column names
            columns = [column[0] for column in cursor.description]

            # Fetch all rows and convert to list of dicts
            rows = cursor.fetchall()
            results = []
            for row in rows:
                row_dict = {}
                for i, col_name in enumerate(columns):
                    row_dict[col_name] = row[i]
                results.append(row_dict)

            return results
        except Exception as e:
            logger.error(f"Database query error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # Execute in thread pool to avoid blocking
    return await asyncio.to_thread(_execute)


async def execute_insert(
    settings: Settings, sql: str, params: tuple[Any, ...] | None = None
) -> dict[str, Any]:
    """
    Execute an INSERT, UPDATE, or DELETE query.

    Args:
        settings: Application settings containing database_connection_string
        sql: SQL query string (use ? placeholders for parameters)
        params: Optional tuple of parameters for parameterized queries

    Returns:
        Dictionary with success status and affected row count:
        {
            "success": bool,
            "rows_affected": int,
            "error": str | None
        }

    Raises:
        Exception: If database connection fails
    """
    if not settings.database_connection_string:
        raise ValueError("database_connection_string is not configured in settings")

    def _execute() -> dict[str, Any]:
        """Execute insert/update/delete synchronously in thread pool."""
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(settings.database_connection_string)
            cursor = conn.cursor()

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            rows_affected = cursor.rowcount
            conn.commit()

            return {
                "success": True,
                "rows_affected": rows_affected,
                "error": None,
            }
        except Exception as e:
            logger.error(f"Database insert/update error: {e}")
            if conn:
                conn.rollback()
            return {
                "success": False,
                "rows_affected": 0,
                "error": str(e),
            }
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # Execute in thread pool to avoid blocking
    return await asyncio.to_thread(_execute)

