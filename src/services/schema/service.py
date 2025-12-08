"""Schema service."""

import logging
from typing import Dict, Any

from src.config.settings import Settings
from src.services.schema.table_selector import TableSelector
from src.infrastructure.cache.schema_cache import SchemaCache
from src.infrastructure.mcp.client import MCPClient

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

    async def close(self):
        """Close MCP client connection (no-op, connections are managed via context manager)."""
        pass

    async def get_schema_context(self, message: str) -> Dict[str, Any]:
        """
        Get schema context for SQL generation.
        
        Args:
            message: User's natural language question
            
        Returns:
            Dictionary with tables array:
            {
                "tables": ["dbo.People", "dbo.Accounts"]
            }
        """
        try:
            # Select relevant tables
            tables = await self.table_selector.select_tables(message)
            
            # Get schema for each table using context manager
            schema_info = {}
            async with MCPClient(self.settings) as mcp_client:
                for table in tables:
                    cache_key = f"schema_{table}"
                    cached = SchemaCache.get(cache_key)
                    
                    if cached:
                        schema_info[table] = cached
                        logger.debug(f"Using cached schema for {table}")
                    else:
                        # Fetch schema from MCP
                        table_schema = await mcp_client.get_table_schema(table)
                        schema_info[table] = table_schema
                        SchemaCache.set(cache_key, table_schema)
                        logger.debug(f"Fetched and cached schema for {table}")

            return {
                "tables": tables,
            }

        except Exception as e:
            logger.error(f"Schema service error: {e}", exc_info=True)
            return {"tables": []}

