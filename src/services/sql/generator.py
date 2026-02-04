"""SQL generator service."""

import hashlib
import logging
from typing import Any, cast

from src.config.prompts import (
    build_sql_generation_system_prompt,
    build_sql_retry_user_input,
)
from src.config.settings import Settings
from src.infrastructure.cache.semantic_cache import SemanticCache
from src.infrastructure.database import DelfosTools
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import (
    azure_agent_client,
    create_anthropic_agent,
    create_anthropic_foundry_agent,
    get_shared_credential,
    is_anthropic_model,
)
from src.infrastructure.mcp.client import mcp_connection
from src.services.sql.models import SQLResult
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generates SQL queries from natural language."""

    def __init__(self, settings: Settings):
        """Initialize SQL generator."""
        self.settings = settings
        logger.info(f"SQLGenerator initialized with model: {settings.sql_agent_model}")

    @staticmethod
    def _generate_cache_key(
        message: str,
        schema_context: dict[str, Any] | None,
        intent: str | None,
        pattern_type: str | None,
    ) -> str:
        """Generate a cache key for SQL generation."""
        normalized_msg = message.lower().strip()
        tables = []
        if schema_context and schema_context.get("tables"):
            tables = sorted(schema_context.get("tables", []))
        cache_data = f"{normalized_msg}|{','.join(tables)}|{intent or ''}|{pattern_type or ''}"
        return hashlib.sha256(cache_data.encode()).hexdigest()

    async def generate(
        self,
        message: str,
        schema_context: dict[str, Any] | None = None,
        intent: str | None = None,
        pattern_type: str | None = None,
        arquetipo: str | None = None,
        previous_errors: list[str] | None = None,
        previous_sql: str | None = None,
        mcp: Any | None = None,
        db_tools: DelfosTools | None = None,
    ) -> dict[str, Any]:
        """
        Generate SQL query from natural language.

        Args:
            message: User's natural language question
            schema_context: Optional schema context with tables and schema info
            intent: Optional intent classification
            pattern_type: Optional pattern type
            arquetipo: Optional archetype (A-N)
            previous_errors: Optional list of validation errors from previous attempt
            previous_sql: Optional SQL query from previous attempt that failed
            mcp: Optional MCP connection
            db_tools: Optional DelfosTools instance for direct DB access

        Returns:
            Dictionary with SQL query and metadata
        """
        try:
            # Only use cache for first attempt (no previous errors)
            use_cache = not (previous_errors and len(previous_errors) > 0)
            cache_key = None

            if use_cache:
                cache_key = self._generate_cache_key(message, schema_context, intent, pattern_type)
                cached_result = SemanticCache.get(cache_key)
                if cached_result is not None:
                    logger.info(f"SQL cache hit for key: {cache_key[:8]}...")
                    return cast(dict[str, Any], cached_result)
                logger.debug(f"SQL cache miss for key: {cache_key[:8]}...")

            prioritized_tables = None
            if schema_context and schema_context.get("tables"):
                prioritized_tables = schema_context.get("tables", [])

            system_prompt = build_sql_generation_system_prompt(
                prioritized_tables=prioritized_tables
            )

            # Build user input
            if previous_errors and len(previous_errors) > 0 and previous_sql:
                user_input = build_sql_retry_user_input(
                    original_question=message,
                    previous_sql=previous_sql,
                    verification_issues=previous_errors,
                    verification_suggestion=None,
                )
            else:
                user_input = message

            model = self.settings.sql_agent_model
            sql_max_tokens = self.settings.sql_max_tokens
            logger.info(f"Using SQL agent model: {model}")

            exploration_tools = [
                "list_tables",
                "get_table_schema",
                "get_table_relationships",
                "get_distinct_values",
                "get_primary_keys",
            ]

            # Determine which tools to use
            if db_tools is not None:
                agent_tools = db_tools.get_exploration_tools()
                logger.info("Using direct DB tools for SQL generation")
            elif mcp is not None:
                agent_tools = mcp
                logger.info("Using provided MCP connection for SQL generation")
            else:
                agent_tools = None  # Will create MCP connection below

            if is_anthropic_model(model):
                if self.settings.use_anthropic_api_for_claude:
                    if not self.settings.anthropic_api_key:
                        raise ValueError(
                            "use_anthropic_api_for_claude is True but ANTHROPIC_API_KEY is not set"
                        )
                    logger.info(f"Using Anthropic API directly for Claude model: {model}")
                    if agent_tools is not None:
                        agent = create_anthropic_agent(
                            settings=self.settings,
                            name="SQLGenerator",
                            instructions=system_prompt,
                            tools=agent_tools,
                            model=model,
                            max_tokens=sql_max_tokens,
                            response_format=SQLResult,
                        )
                        result_model = await run_agent_with_format(
                            agent, user_input, response_format=SQLResult
                        )
                    else:
                        async with mcp_connection(
                            self.settings, allowed_tools=exploration_tools
                        ) as mcp_tool:
                            agent = create_anthropic_agent(
                                settings=self.settings,
                                name="SQLGenerator",
                                instructions=system_prompt,
                                tools=mcp_tool,
                                model=model,
                                max_tokens=sql_max_tokens,
                                response_format=SQLResult,
                            )
                            result_model = await run_agent_with_format(
                                agent, user_input, response_format=SQLResult
                            )
                else:
                    logger.info(f"Using Anthropic on Foundry for Claude model: {model}")
                    if agent_tools is not None:
                        agent = create_anthropic_foundry_agent(
                            settings=self.settings,
                            name="SQLGenerator",
                            instructions=system_prompt,
                            tools=agent_tools,
                            model=model,
                            max_tokens=sql_max_tokens,
                            response_format=SQLResult,
                        )
                        result_model = await run_agent_with_format(
                            agent, user_input, response_format=SQLResult
                        )
                    else:
                        async with mcp_connection(
                            self.settings, allowed_tools=exploration_tools
                        ) as mcp_tool:
                            agent = create_anthropic_foundry_agent(
                                settings=self.settings,
                                name="SQLGenerator",
                                instructions=system_prompt,
                                tools=mcp_tool,
                                model=model,
                                max_tokens=sql_max_tokens,
                                response_format=SQLResult,
                            )
                            result_model = await run_agent_with_format(
                                agent, user_input, response_format=SQLResult
                            )
            else:
                credential = get_shared_credential()
                async with azure_agent_client(self.settings, model, credential) as client:
                    if agent_tools is not None:
                        agent = client.create_agent(
                            name="SQLGenerator",
                            instructions=system_prompt,
                            tools=agent_tools,
                            max_tokens=sql_max_tokens,
                            temperature=self.settings.sql_temperature,
                            response_format=SQLResult,
                        )
                        result_model = await run_agent_with_format(
                            agent, user_input, response_format=SQLResult
                        )
                    else:
                        async with mcp_connection(
                            self.settings, allowed_tools=exploration_tools
                        ) as mcp_tool:
                            agent = client.create_agent(
                                name="SQLGenerator",
                                instructions=system_prompt,
                                tools=mcp_tool,
                                max_tokens=sql_max_tokens,
                                temperature=self.settings.sql_temperature,
                                response_format=SQLResult,
                            )
                            result_model = await run_agent_with_format(
                                agent, user_input, response_format=SQLResult
                            )

            if isinstance(result_model, SQLResult):
                result_dict = result_model.model_dump()
            elif isinstance(result_model, str):
                logger.warning(
                    f"SQL agent returned string instead of SQLResult. Raw response (first 1000 chars): {result_model[:1000]}"
                )
                sql_json = JSONParser.extract_json(result_model)
                if not sql_json:
                    logger.error(
                        f"Could not extract JSON from SQL agent response. Full raw response: {result_model}"
                    )
                    return {
                        "pregunta_original": message,
                        "sql": "",
                        "tablas": [],
                        "resumen": f"Error: Could not parse SQL agent response. Raw response: {result_model[:200]}...",
                    }
                sql_result = SQLResult(**sql_json)
                result_dict = sql_result.model_dump()
            else:
                logger.error(f"Unexpected response type from SQL agent: {type(result_model)}")
                return {
                    "pregunta_original": message,
                    "sql": "",
                    "tablas": [],
                    "resumen": f"Error: Unexpected response type {type(result_model)}",
                }

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
