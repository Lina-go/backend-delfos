"""SQL executor service."""

import logging
from typing import Dict, Any

from src.config.settings import Settings
from src.config.prompts import build_sql_formatting_system_prompt
from src.infrastructure.mcp.client import MCPClient
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import (
    azure_agent_client,
    get_shared_credential,
)
from src.services.sql.models import SQLExecutionResult

logger = logging.getLogger(__name__)


class SQLExecutor:
    """
    Executes SQL queries directly via MCP and formats results using an LLM agent.
    
    This approach prevents infinite loops by:
    1. Executing the SQL query directly (once) via MCPClient
    2. Using an LLM agent (without tools) to format the results
    """

    def __init__(self, settings: Settings):
        """Initialize SQL executor."""
        self.settings = settings

    async def close(self):
        """Close MCP connection (no-op, connections are managed via context manager)."""
        pass

    async def execute(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query directly via MCP and format results using an LLM agent.
        
        Note: Validation should be done before calling this method.
        This method:
        1. Executes the SQL query directly via MCPClient (guaranteed once)
        2. Uses an LLM agent (without MCP tools) to format the raw results
        
        Args:
            sql: SQL query string (should be pre-validated)
            
        Returns:
            Dictionary with results and metadata:
            {
                "resultados": [{"column_name": value, ...}, ...],
                "total_filas": int,
                "resumen": str
            }
        """
        try:
            # Step 1: Execute SQL query directly via MCP (guaranteed to run once)
            logger.info("Executing SQL query directly via MCP (single execution)")
            async with MCPClient(self.settings) as mcp_client:
                execution_result = await mcp_client.execute_sql(sql)
                
                if execution_result.get("error"):
                    logger.error(f"SQL execution error: {execution_result['error']}")
                    return {
                        "resultados": [],
                        "total_filas": 0,
                        "resumen": f"Error executing SQL: {execution_result['error']}",
                        "insights": None,
                    }
                
                raw_results = execution_result.get("raw", "")
                row_count = execution_result.get("row_count", 0)
                data_rows = execution_result.get("data", [])
            
            logger.info(f"SQL executed successfully: {row_count} rows returned")
            
            # Step 2: Format results using LLM agent (NO MCP tools - prevents infinite loops)
            system_prompt = build_sql_formatting_system_prompt()
            model = self.settings.sql_executor_agent_model
            executor_max_tokens = self.settings.sql_executor_max_tokens
            executor_temperature = self.settings.sql_executor_temperature

            # Create user message with SQL query and raw results
            user_message = (
                f"Format the following SQL query results as dictionaries:\n\n"
                f"SQL Query:\n{sql}\n\n"
                f"Raw Results (newline-separated):\n{raw_results}\n\n"
                f"Number of rows: {row_count}"
            )
            
            logger.info(f"Formatting results via agent with model: {model} (no tools)")

            credential = get_shared_credential()
            # SQLFormatter doesn't use tools, so only needs 1-2 iterations
            async with azure_agent_client(
                self.settings, model, credential, max_iterations=2
            ) as client:
                # No MCP tools - agent only formats, cannot execute queries
                agent = client.create_agent(
                    name="SQLFormatter",
                    instructions=system_prompt,
                    tools=None,  # No tools = no infinite loops
                    max_tokens=executor_max_tokens,
                    temperature=executor_temperature,
                )
                result_model = await run_agent_with_format(
                    agent, user_message, response_format=SQLExecutionResult
                )
            
            return result_model.model_dump()

        except Exception as e:
            logger.error(f"SQL execution error: {e}", exc_info=True)
            return {
                "resultados": [],
                "total_filas": 0,
                "resumen": f"Error executing SQL: {str(e)}",
                "insights": None,
            }

