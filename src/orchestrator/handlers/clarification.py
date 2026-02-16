"""Clarification handler - asks the user for more details when the query is ambiguous."""

import logging
from typing import Any

from src.api.response import build_response
from src.config.constants import QueryType
from src.config.settings import Settings
from src.orchestrator.handlers._llm_helper import run_handler_agent

logger = logging.getLogger(__name__)

CLARIFICATION_SYSTEM_PROMPT = """Eres Delfos, un asistente de datos financieros del sistema financiero colombiano.

La pregunta del usuario es ambigua o incompleta. Tu tarea es generar UNA pregunta de clarificacion
para entender exactamente que datos necesita.

## Contexto Disponible
Tienes acceso a datos sobre:
- Cartera de credito: saldos por entidad, tipo de credito (consumo, comercial, vivienda, microcredito)
- Tasas de mercado: tasas de captacion (CDT, cuentas de ahorro), por plazo y tipo de entidad
- Entidades financieras: bancos, companias de financiamiento, cooperativas
- Periodos temporales: datos mensuales historicos

## Instrucciones
1. Analiza por que la pregunta es ambigua (falta entidad, periodo, metrica, tipo de credito, etc.)
2. Genera UNA sola pregunta de clarificacion, concisa y especifica
3. Si es posible, ofrece opciones concretas al usuario
4. Responde SOLO con la pregunta de clarificacion, sin preambulos

## Ejemplos
- Pregunta ambigua: "dame los datos"
  Clarificacion: "Que datos te gustaria consultar? Puedo mostrarte saldos de cartera por entidad, tasas de captacion (CDT, cuentas de ahorro) o informacion de entidades financieras."

- Pregunta ambigua: "comparame los bancos"
  Clarificacion: "Que metrica te gustaria comparar entre los bancos? Por ejemplo: saldo de cartera total, cartera de consumo, tasa de captacion, o mora."

- Pregunta ambigua: "cual es la tasa?"
  Clarificacion: "Que tipo de tasa buscas? Puedo consultar tasas de captacion (CDT a 30, 60, 90 dias, cuentas de ahorro) o tasas de colocacion. Y para que periodo?"
"""


class ClarificationHandler:
    """Handles ambiguous queries by asking the user for clarification."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def handle(self, message: str, conversation_history: str = "") -> dict[str, Any]:
        """Generate a clarification question for an ambiguous query."""
        try:
            prompt = message
            if conversation_history:
                prompt = f"{conversation_history}\n\nPregunta actual del usuario: {message}"

            clarification_question = await run_handler_agent(
                self.settings,
                name="ClarificationAgent",
                instructions=CLARIFICATION_SYSTEM_PROMPT,
                message=prompt,
                max_iterations=1,
                max_tokens=300,
                temperature=0.3,
            )

            return build_response(
                patron=QueryType.NEEDS_CLARIFICATION,
                insight=clarification_question,
                needs_clarification=True,
                clarification_question=clarification_question,
            )

        except Exception as e:
            logger.error("ClarificationHandler error: %s", e, exc_info=True)
            fallback = (
                "Tu pregunta es un poco ambigua. Podrias ser mas especifico? "
                "Por ejemplo, indica que metrica, entidad o periodo te interesa."
            )
            return build_response(
                patron=QueryType.NEEDS_CLARIFICATION,
                insight=fallback,
                needs_clarification=True,
                clarification_question=fallback,
            )
