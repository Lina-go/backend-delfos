"""Triage classifier service."""

import logging
from typing import Any

from src.config.prompts import build_triage_system_prompt
from src.config.settings import Settings
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import (
    azure_agent_client,
    get_shared_credential,
)
from src.infrastructure.mcp.client import mcp_connection
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class TriageClassifier:
    """Classifies queries into data_question, general, or out_of_scope."""

    def __init__(self, settings: Settings):
        """Initialize triage classifier."""
        self.settings = settings

    async def classify(self, message: str, has_context: bool = False) -> dict[str, Any]:
        """
        Classify a user message.

        Args:
            message: User's natural language question
            has_context: Whether the user has previous conversation data

        Returns:
            Dictionary with query_type, confidence, and reasoning
        """
        try:
            system_prompt = build_triage_system_prompt(has_context=has_context)
            model = self.settings.triage_agent_model

            credential = get_shared_credential()
            async with (
                azure_agent_client(self.settings, model, credential, max_iterations=5) as client,
                mcp_connection(self.settings) as mcp,
            ):
                agent = client.create_agent(
                    name="TriageClassifier",
                    instructions=system_prompt,
                    tools=mcp,
                    max_tokens=self.settings.triage_max_tokens,
                    temperature=self.settings.triage_temperature,
                )
                response = await run_single_agent(agent, message)

            result = JSONParser.extract_json(response)
            return result

        except Exception as e:
            logger.error(f"Triage classification error: {e}", exc_info=True)
            return {
                "query_type": "data_question",
                "reasoning": "Error in classification, defaulting to data_question",
            }
