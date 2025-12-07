"""SQL generator service."""

import logging
from typing import Dict, Any, Optional

from src.config.settings import Settings
from src.config.prompts import (
    build_sql_generation_system_prompt,
    build_sql_generation_user_input,
)
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import (

    create_anthropic_agent,
)
from src.infrastructure.mcp.client import mcp_connection
from src.services.sql.models import SQLResult

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generates SQL queries from natural language."""

    def __init__(self, settings: Settings):
        """Initialize SQL generator.
        
        Args:
            settings: Application settings
        """
        self.settings = settings

    async def generate(
        self, 
        message: str,
        schema_context: Optional[Dict[str, Any]] = None,
        intent: Optional[str] = None,
        pattern_type: Optional[str] = None,
        arquetipo: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate SQL query from natural language.
        
        Args:
            message: User's natural language question
            schema_context: Optional schema context with tables and schema info
            intent: Optional intent classification (nivel_puntual, requiere_viz)
            pattern_type: Optional pattern type (A-N)
            arquetipo: Optional analytical archetype
            
        Returns:
            Dictionary with SQL query and metadata
        """
        try:
            # Extract prioritized tables from schema_context if provided
            prioritized_tables = None
            if schema_context and schema_context.get("tables"):
                prioritized_tables = schema_context.get("tables", [])
            
            # Get base system prompt with prioritized tables
            system_prompt = build_sql_generation_system_prompt(prioritized_tables=prioritized_tables)
            
            # Build structured user message with intent context
            user_message = build_sql_generation_user_input(
                message=message,
                intent=intent,
                pattern_type=pattern_type,
                arquetipo=arquetipo
            )

            model = self.settings.sql_agent_model
            sql_max_tokens = self.settings.sql_max_tokens
            sql_temperature = self.settings.sql_temperature

            async with mcp_connection(self.settings) as mcp:
                agent = create_anthropic_agent(
                    settings=self.settings,
                    name="SQLGenerator",
                    instructions=system_prompt,
                    tools=mcp,
                    model=model,
                    max_tokens=sql_max_tokens,
                )
                result_model = await run_agent_with_format(
                    agent, user_message, response_format=SQLResult
                )
            
            # Convert Pydantic model to dict
            if isinstance(result_model, SQLResult):
                return result_model.model_dump()
            else:
                # Fallback if result is not the expected type
                return {
                    "pregunta_original": message,
                    "sql": "",
                    "tablas": [],
                    "resumen": "Error: Unexpected response format",
                }

        except Exception as e:
            logger.error(f"SQL generation error: {e}", exc_info=True)
            return {
                "pregunta_original": message,
                "sql": "",
                "tablas": [],
                "resumen": f"Error generating SQL: {str(e)}",
            }

