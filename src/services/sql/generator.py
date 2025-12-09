"""SQL generator service."""

import hashlib
import logging
from typing import Dict, Any, Optional, List

from src.config.settings import Settings
from src.config.prompts import (
    build_sql_generation_system_prompt,
    build_sql_retry_user_input,
)
from src.infrastructure.cache.semantic_cache import SemanticCache
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import create_anthropic_agent
from src.infrastructure.mcp.client import mcp_connection
from src.services.sql.models import SQLResult
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generates SQL queries from natural language."""

    def __init__(self, settings: Settings):
        """Initialize SQL generator.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
        logger.info(f"SQLGenerator initialized with model: {settings.sql_agent_model}")

    @staticmethod
    def _generate_cache_key(
        message: str,
        schema_context: Optional[Dict[str, Any]],
        intent: Optional[str],
        pattern_type: Optional[str],
    ) -> str:
        """Generate a cache key for SQL generation.
        
        Args:
            message: User's natural language question
            schema_context: Schema context with tables
            intent: Intent classification
            pattern_type: Pattern type
            
        Returns:
            Cache key (MD5 hash)
        """
        # Normalize message (lowercase, strip whitespace)
        normalized_msg = message.lower().strip()
        
        # Get tables (sorted for consistency)
        tables = []
        if schema_context and schema_context.get("tables"):
            tables = sorted(schema_context.get("tables", []))
        
        # Create unique string
        cache_data = f"{normalized_msg}|{','.join(tables)}|{intent or ''}|{pattern_type or ''}"
        
        # Generate MD5 hash for shorter key
        return hashlib.md5(cache_data.encode()).hexdigest()

    async def generate(
        self, 
        message: str,
        schema_context: Optional[Dict[str, Any]] = None,
        intent: Optional[str] = None,
        pattern_type: Optional[str] = None,
        arquetipo: Optional[str] = None,
        previous_errors: Optional[List[str]] = None,
        previous_sql: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate SQL query from natural language.
        
        Args:
            message: User's natural language question
            schema_context: Optional schema context with tables and schema info
            intent: Optional intent classification (nivel_puntual, requiere_visualizacion)
            pattern_type: Optional pattern type (Comparación, Relación, etc.)
            arquetipo: Optional archetype (A-N)
            previous_errors: Optional list of validation errors from previous attempt
            previous_sql: Optional SQL query from previous attempt that failed
            
        Returns:
            Dictionary with SQL query and metadata
        """
        try:
            # Only use cache for first attempt (no previous errors)
            # Retries should always generate new SQL
            use_cache = not (previous_errors and len(previous_errors) > 0)
            cache_key = None
            
            if use_cache:
                # Try to get from cache
                cache_key = self._generate_cache_key(message, schema_context, intent, pattern_type)
                cached_result = SemanticCache.get(cache_key)
                
                if cached_result:
                    logger.info(f"SQL cache hit for key: {cache_key[:8]}...")
                    return cached_result
                else:
                    logger.debug(f"SQL cache miss for key: {cache_key[:8]}...")
            
            # Extract prioritized tables from schema_context if provided
            prioritized_tables = None
            if schema_context and schema_context.get("tables"):
                prioritized_tables = schema_context.get("tables", [])
            
            # Get base system prompt with prioritized tables
            system_prompt = build_sql_generation_system_prompt(prioritized_tables=prioritized_tables)

            # Build user input: use retry format if there are previous errors, otherwise use message directly
            if previous_errors and len(previous_errors) > 0 and previous_sql:
                # Build retry input with previous SQL and errors
                user_input = build_sql_retry_user_input(
                    original_question=message,
                    previous_sql=previous_sql,
                    verification_issues=previous_errors,
                    verification_suggestion=None,
                )
            else:
                # First attempt: use message directly
                user_input = message

            model = self.settings.sql_agent_model
            sql_max_tokens = self.settings.sql_max_tokens
            logger.info(f"Using SQL agent model: {model}")

            # Only allow exploration tools, not execution tools
            # SQLGenerator should only PLAN/GENERATE SQL, not execute it
            exploration_tools = [
                "list_tables",
                "get_table_schema",
                "get_table_relationships",
                "get_distinct_values",
                "get_primary_keys",
            ]

            async with mcp_connection(self.settings, allowed_tools=exploration_tools) as mcp:
                agent = create_anthropic_agent(
                    settings=self.settings,
                    name="SQLGenerator",
                    instructions=system_prompt,
                    tools=mcp,
                    model=model,
                    max_tokens=sql_max_tokens,
                    response_format=SQLResult
                )
                # Execute agent and get raw response
                raw_result = await run_single_agent(agent, user_input)
            
            # Extract JSON from response
            sql_json = JSONParser.extract_json(raw_result)
            
            if not sql_json:
                logger.error(f"Could not extract JSON from SQL agent response. Raw response (first 500 chars): {raw_result[:500]}")
                return {
                    "pregunta_original": message,
                    "sql": "",
                    "tablas": [],
                    "resumen": "Error: Could not parse SQL agent response",
                }
            
            # Validate and return as dict
            sql_result = SQLResult(**sql_json)
            result_dict = sql_result.model_dump()
            
            # Cache the result if this was a first attempt (no previous errors)
            if use_cache and cache_key:
                SemanticCache.set(cache_key, result_dict)
                logger.debug(f"Cached SQL result for key: {cache_key[:8]}...")
            
            return result_dict

        except Exception as e:
            logger.error(f"SQL generation error: {e}", exc_info=True)
            return {
                "pregunta_original": message,
                "sql": "",
                "tablas": [],
                "resumen": f"Error generating SQL: {str(e)}",
            }

