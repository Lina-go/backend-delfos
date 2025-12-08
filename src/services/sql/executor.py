"""SQL executor service."""

import logging
from typing import Dict, Any

from src.config.settings import Settings
from src.config.prompts import build_sql_execution_system_prompt
from src.infrastructure.mcp.client import mcp_connection
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import (
    azure_agent_client,
    get_shared_credential,
)
from src.services.sql.models import SQLExecutionResult

logger = logging.getLogger(__name__)


class SQLExecutor:
    """Executes SQL queries via MCP using an LLM agent to format results."""

    def __init__(self, settings: Settings):
        """Initialize SQL executor."""
        self.settings = settings

    async def close(self):
        """Close any connections (no-op for agent-based executor)."""
        pass

    async def execute(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query via MCP agent and format results.
        
        Note: Validation should be done before calling this method.
        This method uses an LLM agent to execute SQL via MCP and format results.
        
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
            system_prompt = build_sql_execution_system_prompt()
            model = self.settings.sql_executor_agent_model
            executor_max_tokens = self.settings.sql_executor_max_tokens
            executor_temperature = self.settings.sql_executor_temperature

            # Create user message with SQL query
            user_message = f"Execute the following SQL query and format the results:\n\n{sql}"
            
            logger.info(f"Executing SQL via agent with model: {model}")

            credential = get_shared_credential()
            async with azure_agent_client(
                self.settings, model, credential
            ) as client:
                async with mcp_connection(self.settings) as mcp:
                    agent = client.create_agent(
                        name="SQLExecutor",
                        instructions=system_prompt,
                        tools=mcp,
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
            }

