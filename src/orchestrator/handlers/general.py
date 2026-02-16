"""General question handler using LLM."""

import logging
from typing import Any

from src.api.response import build_response
from src.config.constants import QueryType
from src.config.prompts import GENERAL_HANDLER_PROMPT
from src.config.settings import Settings
from src.orchestrator.handlers._llm_helper import run_handler_agent

logger = logging.getLogger(__name__)


class GeneralHandler:
    """Handles general questions about the system using LLM."""

    def __init__(self, settings: Settings):
        """Initialize handler with settings."""
        self.settings = settings

    async def handle(self, message: str) -> dict[str, Any]:
        """Handle a general question using LLM."""
        try:
            system_prompt = self._build_system_prompt()

            response = await run_handler_agent(
                self.settings,
                name="GeneralHandler",
                instructions=system_prompt,
                message=message,
            )

            return build_response(patron=QueryType.GENERAL, insight=response)

        except Exception as e:
            logger.error("GeneralHandler error: %s", e, exc_info=True)
            return build_response(
                patron=QueryType.GENERAL,
                insight="Lo siento, no pude procesar tu pregunta. ¿Puedo ayudarte con algo más?",
                error=str(e),
            )

    def _build_system_prompt(self) -> str:
        """Build system prompt for general questions."""
        return GENERAL_HANDLER_PROMPT
