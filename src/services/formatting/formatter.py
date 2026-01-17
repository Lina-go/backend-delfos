"""Response formatter service with LLM or code-based formatting."""

import logging
from typing import Any

from src.config.prompts import build_format_prompt
from src.config.settings import Settings
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import (
    azure_agent_client,
    get_shared_credential,
)
from src.orchestrator.state import PipelineState
from src.services.formatting.code_formatter import CodeFormatter
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class ResponseFormatter:
    """Formats final response using LLM or code-based formatting."""

    def __init__(self, settings: Settings):
        """Initialize response formatter."""
        self.settings = settings
        self.code_formatter = CodeFormatter()

    async def format(self, state: PipelineState) -> dict[str, Any]:
        """
        Format final response from pipeline state.

        Uses LLM formatting if `use_llm_formatting=True`, otherwise uses code-based formatting.

        Args:
            state: Pipeline state object

        Returns:
            Formatted response dictionary
        """
        if self.settings.use_llm_formatting:
            return await self._format_with_llm(state)
        else:
            return self.code_formatter.format(state)

    async def _format_with_llm(self, state: PipelineState) -> dict[str, Any]:
        """Format response using LLM agent."""
        try:
            # Build input for formatter
            format_input = {
                "pregunta_original": state.user_message,
                "intent": state.intent,
                "tipo_patron": state.pattern_type,
                "arquetipo": state.arquetipo,
                "sql_data": {
                    "pregunta_original": state.user_message,
                    "sql": state.sql_query,
                    "tablas": state.selected_tables,
                    "resultados": state.sql_results or [],
                    "total_filas": state.total_filas,
                    "resumen": state.sql_resumen,
                    "insights": state.sql_insights,
                },
            }

            if state.viz_required and state.powerbi_url:
                format_input["viz_data"] = {
                    "tipo_grafico": state.tipo_grafico,
                    "powerbi_url": state.powerbi_url,
                    "image_url": state.image_url,
                    "html_url": state.html_url,
                    "run_id": state.run_id,
                }

            # Use LLM to format response
            system_prompt = build_format_prompt()
            model = self.settings.format_agent_model

            # Create agent without tools
            credential = get_shared_credential()
            # ResponseFormatter doesn't use tools, only needs 1-2 iterations
            async with azure_agent_client(
                self.settings, model, credential, max_iterations=2
            ) as client:
                agent = client.create_agent(
                    name="ResponseFormatter",
                    instructions=system_prompt,
                    max_tokens=self.settings.format_max_tokens,
                    temperature=self.settings.format_temperature,
                )
                response = await run_single_agent(agent, str(format_input))

            result = JSONParser.extract_json(response)
            return result

        except Exception as e:
            logger.error(f"LLM formatting error: {e}", exc_info=True)
            # Fallback to code-based formatting on error
            return self.code_formatter.format(state)
