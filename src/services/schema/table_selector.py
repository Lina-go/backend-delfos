"""Table selector service."""

import logging

from src.config.database import get_tables_for_query
from src.config.settings import Settings

logger = logging.getLogger(__name__)


class TableSelector:
    """Selects relevant tables based on user message."""

    def __init__(self, settings: Settings):
        """Initialize table selector.

        Args:
            settings: Application settings
        """
        self.settings = settings

    async def select_tables(self, message: str) -> list[str]:
        """
        Select relevant tables based on user message.

        Args:
            message: User's natural language question

        Returns:
            List of relevant table names
        """
        try:
            # Use get_tables_for_query to find tables based on concepts
            tables = get_tables_for_query(message)

            if tables:
                table_list = list(tables)
                logger.debug(f"Selected {len(table_list)} tables for message: {table_list}")
                return table_list

            # Fallback: if no tables found, return empty list
            # The SQL agent can still use MCP tools to discover tables
            logger.warning(f"No tables found for message: {message[:50]}...")
            return []

        except Exception as e:
            logger.error(f"Error selecting tables: {e}", exc_info=True)
            return []
