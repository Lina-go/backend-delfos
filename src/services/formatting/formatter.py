"""Response formatter service with LLM or code-based formatting."""

import logging
from typing import Any

from src.config.prompts import build_format_prompt
from src.config.settings import Settings
from src.orchestrator.handlers._llm_helper import run_handler_agent
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
                    "run_id": state.run_id,
                }

            # Use LLM to format response
            system_prompt = build_format_prompt()

            response = await run_handler_agent(
                self.settings,
                name="ResponseFormatter",
                instructions=system_prompt,
                message=str(format_input),
                model=self.settings.format_agent_model,
                max_tokens=self.settings.format_max_tokens,
                temperature=self.settings.format_temperature,
            )

            result = JSONParser.extract_json(response)
            return result

        except Exception as e:
            logger.error("LLM formatting error: %s", e, exc_info=True)
            # Fallback to code-based formatting on error
            return self.code_formatter.format(state)
