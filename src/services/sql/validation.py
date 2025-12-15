"""SQL validation service."""

import logging
from typing import Any

from src.config.database import DATABASE_TABLES
from src.config.validation import validate_sql_query

logger = logging.getLogger(__name__)


class SQLValidationService:
    """Validates SQL queries using validation.py functions."""

    @staticmethod
    def validate(sql: str) -> dict[str, Any]:
        """
        Validate SQL query using comprehensive validation rules.

        This method uses the validation functions in validation.py
        which perform comprehensive checks including:
        - Security checks (blocked keywords, patterns, schemas)
        - Statement type validation
        - Required prefix validation (dbo.)
        - Optional table reference validation

        Args:
            sql: SQL query string

        Returns:
            Dictionary with validation result:
            {
                "is_valid": bool,
                "errors": list[str]
            }
        """
        # Get valid tables from DATABASE_TABLES for schema validation
        valid_tables = set(DATABASE_TABLES.keys())

        is_valid, errors = validate_sql_query(sql, valid_tables=valid_tables)

        # Log warnings for informational purposes
        if not is_valid:
            logger.warning(f"SQL validation failed with {len(errors)} error(s): {errors}")
        else:
            logger.info("SQL validation passed")

        return {
            "is_valid": is_valid,
            "errors": errors,
        }
