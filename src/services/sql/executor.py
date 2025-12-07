"""SQL executor service."""

import logging
from typing import Dict, Any

from src.config.settings import Settings
from src.infrastructure.mcp.client import MCPClient

logger = logging.getLogger(__name__)


class SQLExecutor:
    """Executes SQL queries via MCP (execute_sql_query tool)."""

    def __init__(self, settings: Settings):
        """Initialize SQL executor."""
        self.settings = settings
        self.mcp_client = MCPClient(settings)

    async def close(self):
        """Close MCP client connection."""
        if hasattr(self.mcp_client, 'close'):
            await self.mcp_client.close()

    async def execute(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query via MCP.
        
        Note: Validation should be done before calling this method.
        This method only executes the query by calling execute_sql_query from MCP.
        
        Args:
            sql: SQL query string (should be pre-validated)
            
        Returns:
            Dictionary with results and metadata
        """
        try:
            # Execute via MCP (calls execute_sql_query tool directly)
            results = await self.mcp_client.execute_sql(sql)

            # Check for SQL execution errors
            if results.get("error"):
                return {
                    "resultados": [],
                    "total_filas": 0,
                    "resumen": f"SQL execution error: {results.get('error')}",
                }

            row_count = results.get("row_count", len(results.get("data", [])))
            return {
                "resultados": results.get("data", []),
                "total_filas": row_count,
                "resumen": f"Query executed successfully. {row_count} rows returned.",
            }

        except Exception as e:
            logger.error(f"SQL execution error: {e}", exc_info=True)
            return {
                "resultados": [],
                "total_filas": 0,
                "resumen": f"Error executing SQL: {str(e)}",
            }

