"""Intent classifier service."""

import logging
from typing import Dict, Any
from azure.identity.aio import DefaultAzureCredential

from src.config.settings import Settings
from src.config.prompts import build_intent_system_prompt
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import (
    azure_agent_client,
)
from src.utils.json_parser import JSONParser
from src.services.intent.models import IntentResult
logger = logging.getLogger(__name__)


class IntentClassifier:
    """Classifies data questions into nivel_puntual or requiere_viz."""

    def __init__(self, settings: Settings):
        """Initialize intent classifier."""
        self.settings = settings
        self._credential = DefaultAzureCredential()

    async def classify(self, message: str) -> Dict[str, Any]:
        """
        Classify a data question's intent.
        
        Args:
            message: User's natural language question
            
        Returns:
            Dictionary with intent, pattern_type, and reasoning
        """
        try:
            system_prompt = build_intent_system_prompt()
            model = self.settings.intent_agent_model
            intent_max_tokens = self.settings.intent_max_tokens
            intent_temperature = self.settings.intent_temperature


            async with azure_agent_client(
                self.settings, model, self._credential
            ) as client:
                agent = client.create_agent(
                    name="IntentClassifier",
                    instructions=system_prompt,
                    tools=None,
                    model=model,
                    max_tokens=intent_max_tokens,
                    temperature=intent_temperature,
                    response_format=IntentResult
                )
                response = await run_single_agent(agent, message)

            result = JSONParser.extract_json(response)
            return result

        except Exception as e:
            logger.error(f"Intent classification error: {e}", exc_info=True)
            return {
                "user_question": message,
                "intent": "error",
                "tipo_patron": "error",
                "arquetipo": "error",
                "razon": f"Error in classification: {str(e)}",
            }

