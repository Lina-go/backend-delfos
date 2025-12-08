"""Intent classifier service."""

import logging
from typing import Dict, Any

from src.config.settings import Settings
from src.config.prompts import build_intent_system_prompt
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import (
    azure_agent_client,
    get_shared_credential,
)
from src.services.intent.models import IntentResult
logger = logging.getLogger(__name__)


class IntentClassifier:
    """Classifies data questions into nivel_puntual or requiere_viz."""

    def __init__(self, settings: Settings):
        """Initialize intent classifier."""
        self.settings = settings

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


            credential = get_shared_credential()
            async with azure_agent_client(
                self.settings, model, credential, max_iterations=2
            ) as client:
                agent = client.create_agent(
                    name="IntentClassifier",
                    instructions=system_prompt,
                    max_tokens=intent_max_tokens,
                    temperature=intent_temperature,
                    response_format=IntentResult,
                )
                result_model = await run_agent_with_format(
                    agent, message, response_format=IntentResult
                )
            
            if not isinstance(result_model, IntentResult):
                # Try to recover if the agent returned raw text
                if isinstance(result_model, str):
                    from src.utils.json_parser import JSONParser
                    json_data = JSONParser.extract_json(result_model)
                    if json_data:
                        try:
                            result_model = IntentResult(**json_data)
                        except Exception as e:
                            logger.error(f"Failed to parse IntentResult from extracted JSON: {e}")
                if not isinstance(result_model, IntentResult):
                    logger.error(f"Unexpected response format from intent classifier: {type(result_model)}")
                    raise ValueError(f"Expected IntentResult, got {type(result_model)}")
            
            return result_model.model_dump()

        except Exception as e:
            logger.error(f"Intent classification error: {e}", exc_info=True)
            return {
                "user_question": message,
                "intent": "error",
                "tipo_patron": "error",
                "arquetipo": "error",
                "razon": f"Error in classification: {str(e)}",
            }

