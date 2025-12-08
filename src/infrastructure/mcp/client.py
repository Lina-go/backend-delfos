"""MCP connection manager."""

import logging
from contextlib import asynccontextmanager
from typing import Any, Optional, Collection

from agent_framework import MCPStreamableHTTPTool, TextContent
from agent_framework.exceptions import ToolExecutionException
from src.config.settings import Settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def mcp_connection(
    settings: Settings, 
    name: str = "delfos-mcp",
    allowed_tools: Optional[Collection[str]] = None
):
    """
    MCP connection as context manager for agent tools.
    
    Usage:
        async with mcp_connection(settings) as mcp:
            agent = client.create_agent(name="SQL", tools=mcp, ...)
        
        # With filtered tools:
        async with mcp_connection(settings, allowed_tools=["list_tables", "get_table_schema"]) as mcp:
            agent = client.create_agent(name="SQL", tools=mcp, ...)
    
    Args:
        settings: Application settings
        name: Name for the MCP tool
        allowed_tools: Optional list of tool names to allow. If None, all tools are available.
        
    Yields:
        MCPStreamableHTTPTool instance ready to use as agent tool
    """
    async with MCPStreamableHTTPTool(
        name=name,
        url=settings.mcp_server_url,
        timeout=settings.mcp_timeout,
        sse_read_timeout=settings.mcp_sse_timeout,
        approval_mode="never_require",
        load_tools=True,
        allowed_tools=allowed_tools,
    ) as mcp:
        yield mcp

# =============================================================================
# MCP Client (para llamadas directas - SQL Execution)
# =============================================================================


class MCPClient:
    """
    Manages MCP connections and direct tool calls.
    
    Two usage patterns:
    
    1. Standalone (manages own connection):
        client = MCPClient(settings)
        await client.connect()
        results = await client.execute_sql("SELECT ...")
        await client.close()
    
    2. With existing connection (from context manager):
        async with mcp_connection(settings) as mcp:
            client = MCPClient.from_connection(mcp)
            results = await client.execute_sql("SELECT ...")
    """

    def __init__(self, settings: Settings):
        """Initialize MCP client.
        
        Args:
            settings: Application settings containing MCP configuration
        """
        self.settings = settings
        self._mcp: MCPStreamableHTTPTool | None = None
        self._owns_connection: bool = True

    @classmethod
    def from_connection(cls, mcp: MCPStreamableHTTPTool) -> "MCPClient":
        """
        Create client from existing MCP connection.
        
        Args:
            mcp: Active MCP connection from context manager
            
        Returns:
            MCPClient instance that uses the provided connection
        """
        instance = cls.__new__(cls)
        instance.settings = None
        instance._mcp = mcp
        instance._owns_connection = False
        return instance

    @property
    def tools(self) -> MCPStreamableHTTPTool | None:
        """Get raw MCP tool for passing to agents.
        
        Returns:
            MCPStreamableHTTPTool instance if connected, None otherwise
        """
        return self._mcp

    async def connect(self):
        """Establish MCP connection."""
        if self._mcp is None and self.settings:
            logger.debug(f"Connecting to MCP server at {self.settings.mcp_server_url}")
            self._mcp = MCPStreamableHTTPTool(
                name="delfos-mcp",
                url=self.settings.mcp_server_url,
                timeout=self.settings.mcp_timeout,
                sse_read_timeout=self.settings.mcp_sse_timeout,
                approval_mode="never_require",
                load_tools=True,
            )
            await self._mcp.__aenter__()
            logger.debug("MCP connection established")

    async def _ensure_connected(self):
        """Ensure MCP connection is established."""
        if self._mcp is None:
            await self.connect()

    def _extract_text_content(self, result: list[Any]) -> str:
        """Extract text content from MCP tool result.
        
        Args:
            result: List of content items from MCP tool call
            
        Returns:
            Combined text content from all TextContent items
        """
        text_parts = []
        for content in result:
            if isinstance(content, TextContent) and content.text:
                text_parts.append(content.text)
        return "\n".join(text_parts)

    async def list_tables(self) -> list[str]:
        """List all tables in the database.
        
        Returns:
            List of table names
            
        Raises:
            ToolExecutionException: If the MCP tool call fails
        """
        await self._ensure_connected()
        
        try:
            # Call the list_tables MCP tool (returns newline-separated table names)
            result = await self._mcp.call_tool("list_tables")
            text = self._extract_text_content(result)
            
            # Server returns: "table1\ntable2\ntable3" or "No tables found in the database."
            if not text or "No tables found" in text:
                return []
            
            # Split by newline and filter empty strings
            tables = [table.strip() for table in text.split("\n") if table.strip()]
            
            logger.debug(f"Retrieved {len(tables)} tables from MCP")
            return tables
            
        except ToolExecutionException as e:
            logger.error(f"Failed to list tables via MCP: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing tables: {e}", exc_info=True)
            raise ToolExecutionException(f"Failed to list tables: {str(e)}", inner_exception=e) from e

    async def get_table_schema(self, table_name: str) -> dict[str, Any]:
        """Get schema for a specific table.
        
        Args:
            table_name: Name of the table to get schema for
            
        Returns:
            Dictionary containing table schema information:
            {
                "name": table_name,
                "columns": [{"name": "col1", "type": "VARCHAR"}, ...]
            }
            
        Raises:
            ToolExecutionException: If the MCP tool call fails
        """
        await self._ensure_connected()
        
        if not table_name:
            raise ValueError("table_name is required")
        
        try:
            # Call the get_table_schema MCP tool
            # Server returns: "COLUMN_NAME: DATA_TYPE\nCOLUMN_NAME: DATA_TYPE" or error message
            result = await self._mcp.call_tool("get_table_schema", table_name=table_name)
            text = self._extract_text_content(result)
            
            # Check for error message
            if "No schema found" in text:
                logger.warning(f"No schema found for table: {table_name}")
                return {"name": table_name, "columns": []}
            
            # Parse schema: "COLUMN_NAME: DATA_TYPE\nCOLUMN_NAME: DATA_TYPE"
            columns = []
            for line in text.split("\n"):
                line = line.strip()
                if line and ":" in line:
                    parts = line.split(":", 1)
                    if len(parts) == 2:
                        col_name = parts[0].strip()
                        col_type = parts[1].strip()
                        columns.append({"name": col_name, "type": col_type})
            
            schema_data = {
                "name": table_name,
                "columns": columns
            }
            
            logger.debug(f"Retrieved schema for table: {table_name} ({len(columns)} columns)")
            return schema_data
            
        except ToolExecutionException as e:
            logger.error(f"Failed to get table schema for {table_name} via MCP: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting table schema: {e}", exc_info=True)
            raise ToolExecutionException(f"Failed to get table schema: {str(e)}", inner_exception=e) from e

    async def execute_sql(self, sql: str) -> dict[str, Any]:
        """Execute SQL query via MCP.
        
        Args:
            sql: SQL query string to execute
            
        Returns:
            Dictionary containing query results:
            {
                "data": [list of result rows as strings],
                "row_count": number of rows,
                "raw": "raw result text",
                "error": None or error message string
            }
            
        Note:
            For critical errors (connection issues, invalid tool calls), 
            exceptions are still raised. The "error" field is for SQL execution errors.
        """
        await self._ensure_connected()
        
        if not sql or not sql.strip():
            raise ValueError("SQL query cannot be empty")
        
        try:
            # Call the execute_sql_query MCP tool (note: server uses execute_sql_query, not execute_sql)
            # Server returns: "row1\nrow2\nrow3" or "No results found."
            result = await self._mcp.call_tool("execute_sql_query", query=sql)
            text = self._extract_text_content(result)
            
            # Server returns newline-separated rows
            if not text or "No results found" in text:
                return {"data": [], "row_count": 0, "raw": text, "error": None}
            
            # Split by newline to get individual rows
            rows = [row.strip() for row in text.split("\n") if row.strip()]
            
            logger.debug(f"SQL executed via MCP: {len(rows)} rows")
            return {"data": rows, "row_count": len(rows), "raw": text, "error": None}
            
        except ToolExecutionException as e:
            logger.error(f"Failed to execute SQL via MCP: {e}")
            # For ToolExecutionException, return error in dict (SQL execution error)
            return {"data": [], "row_count": 0, "raw": "", "error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error executing SQL: {e}", exc_info=True)
            # For unexpected errors, still raise exception (connection issues, etc.)
            raise ToolExecutionException(f"Failed to execute SQL: {str(e)}", inner_exception=e) from e

    async def close(self):
        """Close MCP connection (only if we own it)."""
        if self._mcp and self._owns_connection:
            logger.debug("Closing MCP connection")
            try:
                await self._mcp.__aexit__(None, None, None)
            except Exception as e:
                logger.warning(f"Error closing MCP connection: {e}")
            finally:
                self._mcp = None

