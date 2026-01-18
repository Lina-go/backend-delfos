"""Follow-up handler - responds to questions about previous results."""

import logging
from typing import Any

from src.config.settings import Settings
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import azure_agent_client, get_shared_credential
from src.orchestrator.context import ConversationContext

logger = logging.getLogger(__name__)


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

        return {
            "patron": "follow_up",
            "datos": context.last_results[:5] if context.last_results else [],
            "arquetipo": "NA",
            "visualizacion": "NO",
            "tipo_grafica": context.last_chart_type,
            "imagen": context.last_response.get("imagen") if context.last_response else None,
            "link_power_bi": None,
            "insight": response_text,
        }

    def _build_prompt(self, message: str, context: ConversationContext) -> str:
        """Build prompt with conversation context."""
        results_preview = context.last_results[:5] if context.last_results else "N/A"
        previous_insight = ""
        if context.last_response:
            previous_insight = context.last_response.get("insight", "")

        return f"""El usuario hizo una consulta y ahora tiene una pregunta de seguimiento.

## Consulta Anterior
- Pregunta: {context.last_query}
- SQL ejecutado: {context.last_sql}
- Resultados (primeros 5): {results_preview}
- Respuesta dada: {previous_insight}

## Pregunta de Seguimiento
"{message}"

## Instrucciones
- Responde de forma clara y concisa en español
- Usa el contexto anterior para dar una respuesta informada
- Si pregunta "¿por qué?", explica basándote en los datos
- No inventes datos que no estén en el contexto
"""

    async def _call_llm(self, prompt: str) -> str:
        """Make a simple LLM call for follow-up response."""
        try:
            credential = get_shared_credential()
            async with azure_agent_client(
                self.settings,
                self.settings.triage_agent_model,
                credential,
                max_iterations=1,
            ) as client:
                agent = client.create_agent(
                    name="FollowUpResponder",
                    instructions="Responde preguntas de seguimiento de forma clara y concisa en español.",
                    temperature=0.3,
                )
                response = await run_single_agent(agent, prompt)
            return response
        except Exception as e:
            logger.error(f"Error in follow-up LLM call: {e}")
            return "Lo siento, no pude procesar tu pregunta. ¿Podrías reformularla?"

    def _no_context_response(self) -> dict[str, Any]:
        """Return response when there's no previous context."""
        return {
            "patron": "follow_up",
            "datos": [],
            "arquetipo": "NA",
            "visualizacion": "NO",
            "tipo_grafica": None,
            "imagen": None,
            "link_power_bi": None,
            "insight": "No tengo contexto de una consulta anterior. ¿Podrías hacer primero una consulta de datos?",
        }