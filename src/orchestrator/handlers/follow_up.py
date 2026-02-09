"""Follow-up handler for Delfos NL2SQL Pipeline."""

import json
import logging
from typing import Any

from src.config.settings import Settings
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import azure_agent_client, get_shared_credential
from src.orchestrator.context import ConversationContext

logger = logging.getLogger(__name__)

# Maximum results to include in context (balance between completeness and token cost)
MAX_RESULTS_IN_CONTEXT = 500


class FollowUpHandler:
    """Handles follow-up questions using conversation context."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def handle(self, message: str, context: ConversationContext) -> dict[str, Any]:
        """Handle follow-up question using previous context.

        Args:
            message: The follow-up question from the user
            context: ConversationContext with previous query data

        Returns:
            Response dictionary with insight and metadata
        """
        if not context.last_results:
            return self._no_context_response()

        prompt = self._build_prompt(message, context)
        response_text = await self._call_llm(prompt)

        return {
            "patron": "follow_up",
            "datos": [],
            "arquetipo": "NA",
            "visualizacion": "NO",
            "tipo_grafica": context.last_chart_type,
            "titulo_grafica": context.last_title,
            "imagen": context.last_response.get("imagen") if context.last_response else None,
            "link_power_bi": None,
            "insight": response_text,
        }

    def _build_prompt(self, message: str, context: ConversationContext) -> str:
        """Build prompt with full conversation context."""
        # Include ALL results (up to limit)
        results_to_include = context.last_results[:MAX_RESULTS_IN_CONTEXT] if context.last_results else []
        total_results = len(context.last_results) if context.last_results else 0

        results_json = json.dumps(results_to_include, indent=2, ensure_ascii=False, default=str)

        previous_insight = ""
        if context.last_response:
            previous_insight = context.last_response.get("insight", "")

        truncation_note = ""
        if total_results > MAX_RESULTS_IN_CONTEXT:
            truncation_note = f"\n**Nota**: Mostrando {MAX_RESULTS_IN_CONTEXT} de {total_results} resultados."

        return f"""Eres un asistente experto en analisis de datos financieros colombianos.
El usuario hizo una consulta y ahora tiene una pregunta de seguimiento.

## Consulta Anterior
- **Pregunta original**: {context.last_query}
- **SQL ejecutado**:
```sql
{context.last_sql}
```
- **Tablas**: {', '.join(context.last_tables) if context.last_tables else 'N/A'}
- **Columnas**: {', '.join(context.last_columns) if context.last_columns else 'N/A'}
- **Total resultados**: {total_results}
- **Insight previo**: {previous_insight}

## Datos Disponibles
```json
{results_json}
```
{truncation_note}

## Pregunta del Usuario
"{message}"

## Instrucciones
1. Responde usando UNICAMENTE los datos proporcionados arriba
2. Si la pregunta pide un valor especifico, buscalo en los datos y citalo exactamente
3. Si la pregunta requiere calculo (suma, promedio, maximo), hazlo con los datos disponibles
4. Responde de forma clara y concisa en espanol
5. Para valores monetarios, usa formato con separadores de miles
6. NO inventes datos que no esten en los resultados

## Ejemplos de Respuesta
- Si preguntan "cual fue el saldo de X en Y?": Busca la fila correspondiente y da el valor exacto
- Si preguntan "cual fue el mayor?": Analiza los datos y responde con el valor y la entidad
- Si preguntan "por que?": Explica basandote en los patrones observados en los datos
"""

    async def _call_llm(self, prompt: str) -> str:
        """Make LLM call for follow-up response."""
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
                    instructions=(
                        "Responde preguntas de seguimiento de forma clara y concisa en espanol. "
                        "Basa tu respuesta UNICAMENTE en los datos proporcionados. "
                        "Cita valores especificos cuando sea posible."
                    ),
                )
                result = await run_single_agent(agent, prompt)
                return result or "No pude procesar la pregunta de seguimiento."
        except Exception as e:
            logger.error(f"Error in follow-up LLM call: {e}", exc_info=True)
            return f"Error procesando la pregunta: {str(e)}"

    def _no_context_response(self) -> dict[str, Any]:
        """Response when there's no previous context."""
        return {
            "patron": "follow_up",
            "datos": [],
            "arquetipo": "NA",
            "visualizacion": "NO",
            "tipo_grafica": None,
            "titulo_grafica": None,
            "imagen": None,
            "link_power_bi": None,
            "insight": (
                "No tengo contexto de una consulta anterior. "
                "Por favor, primero haz una consulta sobre los datos financieros."
            ),
        }
