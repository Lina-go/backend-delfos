"""MCP connection manager and client for agent tool calls."""

import logging
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from typing import Any

from agent_framework import MCPStreamableHTTPTool, TextContent
from agent_framework.exceptions import ToolExecutionException

from src.config.settings import Settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def mcp_connection(
    settings: Settings,
    name: str = "delfos-mcp",
    allowed_tools: Collection[str] | None = None,
) -> AsyncIterator[MCPStreamableHTTPTool]:
    """Yield an MCPStreamableHTTPTool connected to the configured MCP server."""
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
# MCP Client
# =============================================================================


class MCPClient:
    """Async MCP client for direct database tool calls."""

    def __init__(self, settings: Settings):
        self.settings: Settings | None = settings
        self._mcp: MCPStreamableHTTPTool | None = None
        self._owns_connection: bool = True

    async def __aenter__(self) -> "MCPClient":
        if self._mcp is None and self.settings:
            await self.connect()
        return self

    async def __aexit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> bool:
        await self.close()
        return False

    @classmethod
    def from_connection(cls, mcp: MCPStreamableHTTPTool) -> "MCPClient":
        """Create a client wrapping an existing MCP connection."""
        instance = cls.__new__(cls)
        instance.settings = None
        instance._mcp = mcp
        instance._owns_connection = False
        return instance

    @property
    def tools(self) -> MCPStreamableHTTPTool | None:
        """Return the underlying MCP tool instance, or None if not connected."""
        return self._mcp

    async def connect(self) -> None:
        """Establish the MCP connection if not already connected."""
        if self._mcp is None and self.settings is not None:
            logger.debug("Connecting to MCP server at %s", self.settings.mcp_server_url)
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

    async def _ensure_connected(self) -> None:
        """Ensure MCP connection is established."""
        if self._mcp is None:
            await self.connect()

    def _extract_text_content(self, result: list[Any]) -> str:
        """Join all TextContent items from an MCP tool result."""
        text_parts = []
        for content in result:
            if isinstance(content, TextContent) and content.text:
                text_parts.append(content.text)
        return "\n".join(text_parts)

    async def _call_tool(self, tool_name: str, **kwargs: Any) -> str:
        """Execute an MCP tool and return its text content."""
        await self._ensure_connected()
        if self._mcp is None:
            raise RuntimeError("MCP connection is not established")
        try:
            result = await self._mcp.call_tool(tool_name, **kwargs)
            return self._extract_text_content(result)
        except ToolExecutionException:
            logger.error("Failed to execute %s via MCP", tool_name)
            raise
        except Exception as e:
            logger.error("Unexpected error executing %s: %s", tool_name, e, exc_info=True)
            raise ToolExecutionException(
                f"Failed to execute {tool_name}: {e}", inner_exception=e
            ) from e

    async def list_tables(self) -> list[str]:
        """List all tables in the database."""
        text = await self._call_tool("list_tables")

        if not text or "No tables found" in text:
            return []

        tables = [table.strip() for table in text.split("\n") if table.strip()]
        logger.debug("Retrieved %s tables from MCP", len(tables))
        return tables

    async def get_table_schema(self, table_name: str) -> dict[str, Any]:
        """Return ``{name, columns: [{name, type}]}`` for a table."""
        if not table_name:
            raise ValueError("table_name is required")

        text = await self._call_tool("get_table_schema", table_name=table_name)

        if "No schema found" in text:
            logger.warning("No schema found for table: %s", table_name)
            return {"name": table_name, "columns": []}

        columns = []
        for line in text.split("\n"):
            line = line.strip()
            if line and ":" in line:
                parts = line.split(":", 1)
                if len(parts) == 2:
                    col_name = parts[0].strip()
                    col_type = parts[1].strip()
                    columns.append({"name": col_name, "type": col_type})

        logger.debug("Retrieved schema for table: %s (%s columns)", table_name, len(columns))
        return {"name": table_name, "columns": columns}

    async def execute_sql(self, sql: str) -> dict[str, Any]:
        """Execute a SQL query via MCP and return a structured result dict."""
        if not sql or not sql.strip():
            raise ValueError("SQL query cannot be empty")

        try:
            text = await self._call_tool("execute_sql_query", query=sql)
        except ToolExecutionException as e:
            return {"data": [], "row_count": 0, "raw": "", "error": str(e)}

        if not text or "No results found" in text:
            return {"data": [], "row_count": 0, "raw": text, "error": None}

        rows = [row.strip() for row in text.split("\n") if row.strip()]
        logger.debug("SQL executed via MCP: %s rows", len(rows))
        return {"data": rows, "row_count": len(rows), "raw": text, "error": None}

    async def insert_agent_output_batch(
        self,
        user_id: str,
        question: str,
        results: list[dict[str, Any]],
        metric_name: str,
        visual_hint: str,
    ) -> str:
        """Insert visualization data and return the generated run_id."""
        logger.debug("Inserting %s data points for user %s", len(results), user_id)

        run_id = await self._call_tool(
            "insert_agent_output_batch",
            user_id=user_id,
            question=question,
            results=results,
            metric_name=metric_name,
            visual_hint=visual_hint,
        )
        logger.info("Successfully inserted batch with run_id: %s", run_id)
        return run_id

    async def generate_powerbi_url(
        self,
        run_id: str,
        visual_hint: str,
    ) -> str:
        """Generate a Power BI report URL filtered by run_id."""
        logger.debug("Generating Power BI URL for run_id: %s", run_id)

        powerbi_url = await self._call_tool(
            "generate_powerbi_url",
            run_id=run_id,
            visual_hint=visual_hint,
        )
        logger.info("Generated Power BI URL for run_id %s", run_id)
        return powerbi_url

    async def close(self) -> None:
        """Close the MCP connection if owned by this client."""
        if self._mcp and self._owns_connection:
            logger.debug("Closing MCP connection")
            try:
                await self._mcp.__aexit__(None, None, None)
            except Exception as e:
                # Suppress cancel scope errors - they occur when context is already closed
                error_msg = str(e)
                if "cancel scope" not in error_msg.lower():
                    logger.warning("Error closing MCP connection: %s", e)
                else:
                    logger.debug("MCP connection already closed (cancel scope): %s", e)
            finally:
                self._mcp = None
