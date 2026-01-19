"""General question handler using LLM."""

import logging
from typing import Any

from src.config.settings import Settings
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import (
    azure_agent_client,
    get_shared_credential,
)

logger = logging.getLogger(__name__)


class GeneralHandler:
    """Handles general questions about the system using LLM."""

    def __init__(self, settings: Settings):
        """Initialize handler with settings."""
        self.settings = settings

    async def handle(self, message: str) -> dict[str, Any]:
        """
        Handle a general question using LLM.

        Args:
            message: User's question

        Returns:
            Response dictionary
        """
        try:
            system_prompt = self._build_system_prompt()
            model = self.settings.triage_agent_model

            credential = get_shared_credential()
            async with azure_agent_client(
                self.settings, model, credential, max_iterations=2
            ) as client:
                agent = client.create_agent(
                    name="GeneralHandler",
                    instructions=system_prompt,
                    max_tokens=1024,
                    temperature=0.7,
                )
                response = await run_single_agent(agent, message)

            return {
                "patron": "general",
                "datos": [],
                "arquetipo": None,
                "visualizacion": "NO",
                "tipo_grafica": None,
                "imagen": None,
                "link_power_bi": None,
                "insight": response,
                "error": "",
            }

        except Exception as e:
            logger.error(f"GeneralHandler error: {e}", exc_info=True)
            return {
                "patron": "general",
                "datos": [],
                "arquetipo": None,
                "visualizacion": "NO",
                "tipo_grafica": None,
                "imagen": None,
                "link_power_bi": None,
                "insight": "Lo siento, no pude procesar tu pregunta. ¿Puedo ayudarte con algo más?",
                "error": str(e),
            }

    def _build_system_prompt(self) -> str:
        """Build system prompt for general questions."""
        return """Eres Delfos, un asistente experto en datos financieros del sistema financiero colombiano.

## Tu Rol
Ayudas a los usuarios a consultar y analizar datos de la Superintendencia Financiera de Colombia.

## Datos Disponibles
Tienes acceso a información sobre:
- **Cartera de crédito**: Saldos por entidad, tipo de crédito (consumo, comercial, vivienda, microcrédito), evolución temporal
- **Tasas de mercado**: Tasas de captación (CDT, cuentas de ahorro), tasas por plazo y tipo de entidad
- **Entidades financieras**: Bancos, compañías de financiamiento, cooperativas

## Tipos de Preguntas que Puedes Responder
1. **Comparaciones**: "¿Cómo se compara el saldo de cartera entre Bancolombia y Davivienda?"
2. **Evolución temporal**: "¿Cómo ha evolucionado la cartera de consumo en el último año?"
3. **Participación de mercado**: "¿Qué participación tiene cada banco en el total de cartera?"
4. **Rankings**: "¿Cuáles son los 5 bancos con mayor cartera?"
5. **Tasas**: "¿Cuál es la tasa promedio de CDT a 90 días?"

## Instrucciones
- Responde en español de manera amigable y profesional
- Si el usuario pregunta qué puedes hacer, explica tus capacidades
- Si el usuario hace una pregunta fuera de tu alcance (clima, deportes, etc.), indica amablemente que solo manejas datos financieros
- Sugiere ejemplos de preguntas que el usuario podría hacer
- Sé conciso pero informativo

## Ejemplo de Respuesta
Si preguntan "¿Qué puedo preguntarte?":
"Puedo ayudarte con:

• Consultar saldos de cartera por entidad o tipo de crédito
• Ver la evolución temporal de métricas financieras
• Comparar entidades del sistema financiero
• Analizar tasas de captación (CDT, cuentas de ahorro)

Por ejemplo, podrías preguntarme: '¿Cómo ha evolucionado el saldo total de cartera en el último año?' o '¿Cuáles son los 5 bancos con mayor participación en cartera de consumo?'"
"""