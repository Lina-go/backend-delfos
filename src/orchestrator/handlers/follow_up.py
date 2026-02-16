"""Follow-up handler for Delfos NL2SQL Pipeline."""

import json
import logging
from typing import Any

from src.api.response import build_response
from src.config.prompts import FOLLOW_UP_PROMPT_TEMPLATE
from src.config.settings import Settings
from src.orchestrator.context import ConversationContext
from src.orchestrator.handlers._llm_helper import run_handler_agent

logger = logging.getLogger(__name__)

# Maximum results to include in context (balance between completeness and token cost)
MAX_RESULTS_IN_CONTEXT = 500


class FollowUpHandler:
    """Handles follow-up questions using conversation context."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def handle(self, message: str, context: ConversationContext) -> dict[str, Any]:
        """Handle follow-up question using previous context."""
        if not context.last_results:
            return self._no_context_response()

        prompt = self._build_prompt(message, context)
        response_text = await self._call_llm(prompt)

        return build_response(
            patron="follow_up",
            arquetipo="NA",
            tipo_grafica=context.last_chart_type,
            titulo_grafica=context.last_title,
            insight=response_text,
        )

    def _build_prompt(self, message: str, context: ConversationContext) -> str:
        """Build prompt with full conversation context."""
        results_to_include = (
            context.last_results[:MAX_RESULTS_IN_CONTEXT] if context.last_results else []
        )
        total_results = len(context.last_results) if context.last_results else 0

        results_json = json.dumps(results_to_include, indent=2, ensure_ascii=False, default=str)

        previous_insight = ""
        if context.last_response:
            previous_insight = context.last_response.get("insight", "")

        truncation_note = ""
        if total_results > MAX_RESULTS_IN_CONTEXT:
            truncation_note = (
                f"\n**Nota**: Mostrando {MAX_RESULTS_IN_CONTEXT} de {total_results} resultados."
            )

        conversation_history = context.get_history_summary()

        return FOLLOW_UP_PROMPT_TEMPLATE.format(
            last_query=context.last_query,
            last_sql=context.last_sql,
            tables=", ".join(context.last_tables) if context.last_tables else "N/A",
            columns=", ".join(context.last_columns) if context.last_columns else "N/A",
            total_results=total_results,
            previous_insight=previous_insight,
            results_json=results_json,
            truncation_note=truncation_note,
            conversation_history=conversation_history,
            message=message,
        )

    async def _call_llm(self, prompt: str) -> str:
        """Make LLM call for follow-up response."""
        try:
            result = await run_handler_agent(
                self.settings,
                name="FollowUpResponder",
                instructions=(
                    "Responde preguntas de seguimiento de forma clara y concisa en espanol. "
                    "Basa tu respuesta UNICAMENTE en los datos proporcionados. "
                    "Cita valores especificos cuando sea posible."
                ),
                message=prompt,
                max_iterations=1,
            )
            return result or "No pude procesar la pregunta de seguimiento."
        except Exception as e:
            logger.error("Error in follow-up LLM call: %s", e, exc_info=True)
            return f"Error procesando la pregunta: {str(e)}"

    def _no_context_response(self) -> dict[str, Any]:
        """Response when there's no previous context."""
        return build_response(
            patron="follow_up",
            arquetipo="NA",
            insight=(
                "No tengo contexto de una consulta anterior. "
                "Por favor, primero haz una consulta sobre los datos financieros."
            ),
        )
