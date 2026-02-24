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
    create_claude_agent,
    get_shared_credential,
    is_anthropic_model,
)
from src.services.sql.models import SQLResult
from src.utils.json_parser import JSONParser
from src.utils.tool_resolver import resolve_agent_tools

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generates SQL queries from natural language."""

    def __init__(self, settings: Settings):
        """Initialize SQL generator."""
        self.settings = settings
        logger.info("SQLGenerator initialized with model: %s", settings.sql_agent_model)

    @staticmethod
    def _generate_cache_key(
        message: str,
        schema_context: dict[str, Any] | None,
        intent: str | None,
        pattern_type: str | None,
        sub_type: str | None = None,
        system_prompt_override: str | None = None,
    ) -> str:
        """Generate a cache key for SQL generation."""
        normalized_msg = message.lower().strip()
        tables = sorted(schema_context["tables"]) if schema_context and schema_context.get("tables") else []
        prompt_hash = (
            hashlib.sha256(system_prompt_override.encode()).hexdigest()[:16]
            if system_prompt_override
            else ""
        )
        parts = [
            normalized_msg,
            ",".join(tables),
            intent or "",
            pattern_type or "",
            sub_type or "",
            prompt_hash,
        ]
        return hashlib.sha256("|".join(parts).encode()).hexdigest()

    async def generate(
        self,
        message: str,
        schema_context: dict[str, Any] | None = None,
        intent: str | None = None,
        pattern_type: str | None = None,
        arquetipo: str | None = None,
        temporality: str | None = None,
        previous_errors: list[str] | None = None,
        previous_sql: str | None = None,
        db_tools: DelfosTools | None = None,
        system_prompt_override: str | None = None,
        sub_type: str | None = None,
    ) -> dict[str, Any]:
        """
        Generate SQL query from natural language.

        Args:
            message: User's natural language question
            schema_context: Optional schema context with tables and schema info
            intent: Optional intent classification
            pattern_type: Optional pattern type
            arquetipo: Optional archetype (A-K)
            temporality: Optional temporality ("temporal" or "estatico")
            previous_errors: Optional list of validation errors from previous attempt
            previous_sql: Optional SQL query from previous attempt that failed
            db_tools: Optional DelfosTools instance for direct DB access
            system_prompt_override: Optional pre-built system prompt (e.g. enriched by pattern hooks)
            sub_type: Optional sub-type for cache key differentiation

        Returns:
            Dictionary with SQL query and metadata
        """
        try:
            # Only use cache for first attempt (no previous errors)
            use_cache = not previous_errors
            cache_key = None

            if use_cache:
                cache_key = self._generate_cache_key(
                    message, schema_context, intent, pattern_type, sub_type,
                    system_prompt_override=system_prompt_override,
                )
                cached_result = SemanticCache.get(cache_key)
                if cached_result is not None:
                    logger.info("SQL cache hit for key: %s...", cache_key[:8])
                    return cast(dict[str, Any], cached_result)
                logger.debug("SQL cache miss for key: %s...", cache_key[:8])

            if system_prompt_override:
                system_prompt = system_prompt_override
            else:
                prioritized_tables = (
                    schema_context["tables"]
                    if schema_context and schema_context.get("tables")
                    else None
                )
                system_prompt = build_sql_generation_system_prompt(
                    prioritized_tables=prioritized_tables,
                    temporality=temporality,
                )

            # Build user input
            if previous_errors and previous_sql:
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
            logger.info("Using SQL agent model: %s", model)

            agent_tools = resolve_agent_tools(db_tools, context="sql_generation")

            if is_anthropic_model(model):
                logger.info("Using Claude agent for model: %s", model)
                agent = create_claude_agent(
                    settings=self.settings,
                    name="SQLGenerator",
                    instructions=system_prompt,
                    tools=agent_tools or [],
                    model=model,
                    max_tokens=sql_max_tokens,
                    response_format=SQLResult,
                )
                result_model = await run_agent_with_format(
                    agent, user_input, response_format=SQLResult
                )
            else:
                credential = get_shared_credential(self.settings)
                async with azure_agent_client(self.settings, model, credential) as client:
                    agent = client.create_agent(
                        name="SQLGenerator",
                        instructions=system_prompt,
                        tools=agent_tools or [],
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
                    "SQL agent returned string instead of SQLResult. Raw response (first 1000 chars): %s",
                    result_model[:1000],
                )
                sql_json = JSONParser.extract_json(result_model)
                if not sql_json:
                    logger.error(
                        "Could not extract JSON from SQL agent response. Full raw response: %s",
                        result_model,
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
                logger.error("Unexpected response type from SQL agent: %s", type(result_model))
                return {
                    "pregunta_original": message,
                    "sql": "",
                    "tablas": [],
                    "resumen": f"Error: Unexpected response type {type(result_model)}",
                }

            if use_cache and cache_key:
                SemanticCache.set(cache_key, result_dict)
                logger.debug("Cached SQL result for key: %s...", cache_key[:8])

            return result_dict

        except Exception as e:
            logger.error("SQL generation error: %s", e, exc_info=True)
            return {
                "pregunta_original": message,
                "sql": "",
                "tablas": [],
                "resumen": f"Error generating SQL: {str(e)}",
            }
