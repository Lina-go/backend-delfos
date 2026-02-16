"""Schema service."""

import logging
from typing import Any

from src.config.settings import Settings
from src.infrastructure.cache.schema_cache import SchemaCache
from src.infrastructure.database import DelfosTools
from src.services.schema.table_selector import TableSelector

logger = logging.getLogger(__name__)


class SchemaService:
    """Service for schema extraction and enrichment."""

    def __init__(self, settings: Settings):
        """Initialize schema service.

        Args:
            settings: Application settings
        """
        self.settings = settings
        self.table_selector = TableSelector(settings)

    async def get_schema_context(
        self,
        message: str,
        db_tools: DelfosTools | None = None,
    ) -> dict[str, Any]:
        """
        Get schema context for SQL generation.

        Args:
            message: User's natural language question
            db_tools: Optional DelfosTools instance for direct DB access

        Returns:
            Dictionary with tables array
        """
        try:
            # Select relevant tables
            tables = await self.table_selector.select_tables(message)

            # Get schema for each table
            schema_info = {}

            if db_tools is not None:
                for table in tables:
                    cache_key = f"schema_{table}"
                    cached = SchemaCache.get(cache_key)

                    if cached:
                        schema_info[table] = cached
                        logger.debug("Using cached schema for %s", table)
                    else:
                        table_schema = db_tools.get_schema(table)
                        schema_info[table] = table_schema
                        SchemaCache.set(cache_key, table_schema)
                        logger.debug("Fetched and cached schema for %s (direct DB)", table)

            return {
                "tables": tables,
            }

        except Exception as e:
            logger.error("Schema service error: %s", e, exc_info=True)
            return {"tables": [], "error": str(e)}
