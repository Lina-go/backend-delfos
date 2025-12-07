"""Triage classifier service."""

import logging
from typing import Dict, Any, Optional
from src.config.settings import Settings
from src.config.prompts import build_triage_system_prompt
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import (
    is_anthropic_model,
    azure_agent_client,
    create_anthropic_agent,
    get_shared_credential,
)
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class TriageClassifier:
    """Classifies queries into data_question, general, or out_of_scope."""

    def __init__(self, settings: Settings):
        """Initialize triage classifier."""
        self.settings = settings

    async def classify(self, message: str) -> Dict[str, Any]:
        """
        Classify a user message.
        
        Args:
            message: User's natural language question
            
        Returns:
            Dictionary with query_type, confidence, and reasoning
        """
        try:
            system_prompt = build_triage_system_prompt()
            model = self.settings.triage_agent_model

            # Create agent without tools
            if is_anthropic_model(model):
                agent = create_anthropic_agent(
                    settings=self.settings,
                    name="TriageClassifier",
                    instructions=system_prompt,
                    tools=None,
                    model=model,
                )
                response = await run_single_agent(agent, message)
            else:
                credential = get_shared_credential()
                async with azure_agent_client(
                    self.settings, model, credential
                ) as client:
                    agent = client.create_agent(
                        name="TriageClassifier",
                        instructions=system_prompt,
                        max_tokens=self.settings.triage_max_tokens,
                        temperature=self.settings.triage_temperature,
                    )
                    response = await run_single_agent(agent, message)

            result = JSONParser.extract_json(response)
            return result

        except Exception as e:
            logger.error(f"Triage classification error: {e}", exc_info=True)
            # Default to data_question on error
            return {
                "query_type": "data_question",
                "confidence": 0.5,
                "reasoning": "Error in classification, defaulting to data_question",
            }

