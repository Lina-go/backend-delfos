"""Result verifier service."""

import logging
from typing import Any

from src.config.prompts import (
    build_verification_system_prompt,
    build_verification_user_input,
)
from src.config.settings import Settings
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import (
    azure_agent_client,
    create_anthropic_agent,
    get_shared_credential,
    is_anthropic_model,
)
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class ResultVerifier:
    """Verifies that SQL results make sense."""

    def __init__(self, settings: Settings):
        """Initialize result verifier."""
        self.settings = settings

    async def verify(self, results: list[dict[str, Any]], sql: str, question: str = "") -> bool:
        """
        Verify SQL results.

        Uses LLM verification if `use_llm_verification=True`, otherwise uses code-based validation.

        Args:
            results: SQL query results
            sql: SQL query that was executed
            question: Original user question (required for LLM verification)

        Returns:
            True if results are valid, False otherwise
        """
        if self.settings.use_llm_verification:
            return await self._verify_with_llm(results, sql, question)
        else:
            return await self._verify_with_code(results)

    async def _verify_with_code(self, results: list[dict[str, Any]]) -> bool:
        """Verify results using code-based validation."""
        try:
            # Basic validation
            if not results:
                logger.warning("No results returned from query")
                return False

            # Check for reasonable number of results
            if len(results) > 10000:
                logger.warning(f"Very large result set: {len(results)} rows")
                return False

            return True

        except Exception as e:
            logger.error(f"Verification error: {e}", exc_info=True)
            return False

    async def _verify_with_llm(
        self, results: list[dict[str, Any]], sql: str, question: str
    ) -> bool:
        """Verify results using LLM agent."""
        try:
            # Format results as string
            results_str = str(results) if results else "No results"

            # Build input
            user_input = build_verification_user_input(question, sql, results_str)
            system_prompt = build_verification_system_prompt()
            model = self.settings.verification_agent_model

            # Create agent
            if is_anthropic_model(model):
                agent = create_anthropic_agent(
                    settings=self.settings,
                    name="ResultVerifier",
                    instructions=system_prompt,
                    tools=None,
                    model=model,
                    max_tokens=self.settings.verification_max_tokens,
                )
                response = await run_single_agent(agent, user_input)
            else:
                credential = get_shared_credential()
                # ResultVerifier doesn't use tools, only needs 1-2 iterations
                async with azure_agent_client(
                    self.settings, model, credential, max_iterations=2
                ) as client:
                    agent = client.create_agent(
                        name="ResultVerifier",
                        instructions=system_prompt,
                        max_tokens=self.settings.verification_max_tokens,
                        temperature=self.settings.verification_temperature,
                    )
                    response = await run_single_agent(agent, user_input)

            # Parse response
            result = JSONParser.extract_json(response)
            is_valid = bool(result.get("is_valid", False))

            if not is_valid:
                issues = result.get("issues", [])
                logger.warning(f"Verification failed: {issues}")

            return is_valid

        except Exception as e:
            logger.error(f"LLM verification error: {e}", exc_info=True)
            # Fallback to code-based verification on error
            return await self._verify_with_code(results)
