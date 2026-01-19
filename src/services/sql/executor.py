"""SQL executor service."""

import logging
from typing import Any

from src.config.prompts import build_sql_formatting_system_prompt
from src.config.settings import Settings
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import (
    azure_agent_client,
    get_shared_credential,
)
from src.infrastructure.mcp.client import MCPClient
from src.services.sql.models import SQLExecutionResult
from src.utils.retry import run_with_retry

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

    async def close(self) -> None:
        """Close MCP connection (no-op, connections are managed via context manager)."""
        pass

    async def execute(self, sql: str) -> dict[str, Any]:
        """
        Execute SQL query directly via MCP and format results using an LLM agent.
        """
        # Max size for raw results to send to LLM (500KB to be safe under 1MB API limit)
        MAX_RESULT_SIZE = 100000
        
        try:
            # Step 1: Execute SQL query directly via MCP with retry logic
            logger.info("Executing SQL query directly via MCP")

            async def _execute_safe() -> dict[str, Any]:
                """Execute SQL once via MCP, raising on connection timeouts."""
                async with MCPClient(self.settings) as mcp_client:
                    result = await mcp_client.execute_sql(sql)

                error_msg = str(result.get("error") or "").lower()
                if "login timeout" in error_msg or "timeout expired" in error_msg:
                    raise Exception(f"SQL Connection Timeout: {result['error']}")

                return result

            try:
                execution_result = await run_with_retry(
                    _execute_safe, max_retries=3, initial_delay=2.0, backoff_factor=2.0
                )
            except Exception as e:
                logger.error(f"SQL execution failed after retries: {e}")
                return {
                    "resultados": [],
                    "total_filas": 0,
                    "resumen": f"Error executing SQL (after retries): {str(e)}",
                    "insights": None,
                }

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

            logger.info(f"SQL executed successfully: {row_count} rows returned")

            # TRUNCATE raw results if too large for LLM API
            results_truncated = False
            if len(raw_results) > MAX_RESULT_SIZE:
                original_size = len(raw_results)
                raw_results = raw_results[:MAX_RESULT_SIZE] + "\n... [TRUNCATED]"
                results_truncated = True
                logger.warning(
                    f"Results truncated from {original_size:,} to {MAX_RESULT_SIZE:,} chars "
                    f"({row_count} rows total)"
                )

            # Step 2: Format results using LLM agent (NO MCP tools)
            system_prompt = build_sql_formatting_system_prompt()
            model = self.settings.sql_executor_agent_model
            executor_max_tokens = self.settings.sql_executor_max_tokens
            executor_temperature = self.settings.sql_executor_temperature

            truncation_note = (
                f"\n\nNOTE: Results were truncated for processing. "
                f"Showing partial data from {row_count} total rows."
                if results_truncated else ""
            )
            
            user_message = (
                f"Format the following SQL query results as dictionaries:\n\n"
                f"SQL Query:\n{sql}\n\n"
                f"Raw Results (newline-separated):\n{raw_results}\n\n"
                f"Number of rows: {row_count}{truncation_note}"
            )

            logger.info(f"Formatting results via agent with model: {model} (no tools)")

            credential = get_shared_credential()
            async with azure_agent_client(
                self.settings, model, credential, max_iterations=2
            ) as client:
                agent = client.create_agent(
                    name="SQLFormatter",
                    instructions=system_prompt,
                    tools=None,
                    max_tokens=executor_max_tokens,
                    temperature=executor_temperature,
                )
                result_model = await run_agent_with_format(
                    agent, user_message, response_format=SQLExecutionResult
                )

            if isinstance(result_model, SQLExecutionResult):
                return result_model.model_dump()

            logger.error("SQL formatter returned unexpected raw text instead of SQLExecutionResult")
            return {
                "resultados": [],
                "total_filas": 0,
                "resumen": "Error formatting SQL results: unexpected raw text response",
                "insights": None,
            }

        except Exception as e:
            logger.error(f"SQL execution error: {e}", exc_info=True)
            return {
                "resultados": [],
                "total_filas": 0,
                "resumen": f"Error executing SQL: {str(e)}",
                "insights": None,
            }