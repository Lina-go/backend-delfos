"""Triage classifier service."""

import logging
from typing import Any

from src.config.prompts import build_triage_system_prompt
from src.config.settings import Settings
from src.orchestrator.handlers._llm_helper import run_handler_agent
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class TriageClassifier:
    """Classifies queries into data_question, general, out_of_scope, follow_up, etc."""

    def __init__(self, settings: Settings):
        """Initialize triage classifier."""
        self.settings = settings

    async def classify(
        self,
        message: str,
        has_context: bool = False,
        context_summary: str | None = None,
        conversation_history: str | None = None,
        db_tools: Any | None = None,
    ) -> dict[str, Any]:
        """
        Classify a user message.

        Args:
            message: User's natural language question
            has_context: Whether the user has previous conversation data
            context_summary: Summary of what data is available in context
            conversation_history: Formatted conversation history
            db_tools: Unused, kept for interface compatibility

        Returns:
            Dictionary with query_type and reasoning
        """
        try:
            system_prompt = build_triage_system_prompt(
                has_context=has_context,
                context_summary=context_summary,
                conversation_history=conversation_history,
            )

            response = await run_handler_agent(
                self.settings,
                name="TriageClassifier",
                instructions=system_prompt,
                message=message,
                model=self.settings.triage_agent_model,
                tools=[],
                max_iterations=1,
                max_tokens=self.settings.triage_max_tokens,
                temperature=self.settings.triage_temperature,
            )

            result = JSONParser.extract_json(response)
            return result

        except Exception as e:
            logger.error("Triage classification error: %s", e, exc_info=True)
            return {
                "query_type": "data_question",
                "reasoning": "Error in classification, defaulting to data_question",
            }
