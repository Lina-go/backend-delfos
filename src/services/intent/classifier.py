"""Intent classifier service."""

import logging
from typing import Any

from src.config.prompts import build_intent_system_prompt
from src.config.settings import Settings
from src.orchestrator.handlers._llm_helper import run_formatted_handler_agent
from src.services.intent.models import IntentResult
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class IntentClassifier:
    """Classifies data questions into nivel_puntual or requiere_viz."""

    def __init__(self, settings: Settings):
        """Initialize intent classifier."""
        self.settings = settings

    async def classify(self, message: str) -> dict[str, Any]:
        """
        Classify a data question's intent.

        Args:
            message: User's natural language question

        Returns:
            Dictionary with intent, pattern_type, and reasoning
        """
        try:
            system_prompt = build_intent_system_prompt()

            result_model = await run_formatted_handler_agent(
                self.settings,
                name="IntentClassifier",
                instructions=system_prompt,
                message=message,
                response_format=IntentResult,
                model=self.settings.intent_agent_model,
                max_tokens=self.settings.intent_max_tokens,
                temperature=self.settings.intent_temperature,
            )

            if not isinstance(result_model, IntentResult):
                # Try to recover if the agent returned raw text
                if isinstance(result_model, str):
                    json_data = JSONParser.extract_json(result_model)
                    if json_data:
                        try:
                            result_model = IntentResult(**json_data)
                        except Exception as e:
                            logger.error("Failed to parse IntentResult from extracted JSON: %s", e)
                if not isinstance(result_model, IntentResult):
                    logger.error(
                        "Unexpected response format from intent classifier: %s", type(result_model)
                    )
                    raise ValueError(f"Expected IntentResult, got {type(result_model)}")

            return result_model.model_dump()

        except Exception as e:
            logger.error("Intent classification error: %s", e, exc_info=True)
            # Return a valid fallback using archetype "A" (nivel_puntual) as default
            # This prevents downstream errors when converting arquetipo to Archetype enum
            return {
                "user_question": message,
                "intent": "nivel_puntual",
                "tipo_patron": "Comparación",
                "arquetipo": "A",
                "razon": f"Error in classification: {str(e)}",
                "temporality": "estatico",
                "subject_cardinality": 1,
            }
