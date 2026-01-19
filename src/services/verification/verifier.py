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
    get_shared_credential,
)
from src.services.verification.verification_result import VerificationResult
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class ResultVerifier:
    """Verifies that SQL results make sense."""

    def __init__(self, settings: Settings):
        """Initialize result verifier."""
        self.settings = settings

    async def verify(
        self, results: list[dict[str, Any]], sql: str, question: str = ""
    ) -> VerificationResult:
        """
        Verify SQL results.

        Uses LLM verification if `use_llm_verification=True`, otherwise uses code-based validation.

        Args:
            results: SQL query results
            sql: SQL query that was executed
            question: Original user question (required for LLM verification)

        Returns:
            VerificationResult with passed status and feedback
        """
        if self.settings.use_llm_verification:
            return await self._verify_with_llm(results, sql, question)
        else:
            return await self._verify_with_code(results, sql)

    async def _verify_with_code(
        self, results: list[dict[str, Any]], sql: str
    ) -> VerificationResult:
        """Verify results using code-based validation."""
        try:
            # Basic validation - 0 results
            if not results:
                logger.warning("No results returned from query")
                return VerificationResult(
                    passed=False,
                    issues=["Query returned 0 rows - check filter values and table/column names"],
                    suggestion=(
                        "The query returned no results. Consider: "
                        "1) Check if filter values match actual data (use SELECT DISTINCT to explore), "
                        "2) Verify column names are correct, "
                        "3) Check if date ranges include available data"
                    ),
                    summary="La consulta no devolvió resultados",
                )

            # Check for reasonable number of results
            if len(results) > 10000:
                logger.warning(f"Very large result set: {len(results)} rows")
                return VerificationResult(
                    passed=False,
                    issues=[f"Result set too large: {len(results)} rows"],
                    suggestion="Add more specific filters or use TOP/LIMIT to restrict results",
                    summary=f"Conjunto de resultados muy grande: {len(results)} filas",
                )

            return VerificationResult(
                passed=True,
                issues=[],
                suggestion=None,
                insight=f"Query returned {len(results)} rows",
                summary=f"Verificación exitosa: {len(results)} filas",
            )

        except Exception as e:
            logger.error(f"Verification error: {e}", exc_info=True)
            return VerificationResult(
                passed=False,
                issues=[f"Verification error: {str(e)}"],
                suggestion=None,
                summary=f"Error de verificación: {str(e)}",
            )

    async def _verify_with_llm(
        self, results: list[dict[str, Any]], sql: str, question: str
    ) -> VerificationResult:
        """Verify results using LLM agent."""
        try:
            # Format results as string
            results_str = str(results) if results else "No results (0 rows)"

            # Build input
            user_input = build_verification_user_input(question, sql, results_str)
            system_prompt = build_verification_system_prompt()
            model = self.settings.verification_agent_model

            # Create agent
            credential = get_shared_credential()
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
            issues = result.get("issues", [])
            suggestion = result.get("suggestion")
            insight = result.get("insight")
            summary = result.get("summary")

            if not is_valid:
                logger.warning(f"LLM Verification failed: {issues}")

            return VerificationResult(
                passed=is_valid,
                issues=issues if isinstance(issues, list) else [str(issues)],
                suggestion=suggestion,
                insight=insight,
                summary=summary,
            )

        except Exception as e:
            logger.error(f"LLM verification error: {e}", exc_info=True)
            # Fallback to code-based verification on error
            return await self._verify_with_code(results, sql)