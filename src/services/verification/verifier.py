"""Result verifier service."""

import logging
import re
from typing import Any

from src.config.prompts import (
    build_verification_system_prompt,
    build_verification_user_input,
)
from src.config.settings import Settings
from src.orchestrator.handlers._llm_helper import run_handler_agent
from src.services.verification.verification_result import VerificationResult
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)


class ResultVerifier:
    """Verifies that SQL results make sense."""

    def __init__(self, settings: Settings):
        """Initialize result verifier."""
        self.settings = settings

    async def verify(
        self,
        results: list[dict[str, Any]],
        sql: str,
        question: str = "",
        execution_error: str | None = None,
    ) -> VerificationResult:
        """
        Verify SQL results.

        Uses LLM verification if `use_llm_verification=True`, otherwise uses code-based validation.

        Args:
            results: SQL query results
            sql: SQL query that was executed
            question: Original user question (required for LLM verification)
            execution_error: Error message from SQL execution (if any)

        Returns:
            VerificationResult with passed status and feedback
        """
        # Execution errors are always handled by code-based verification
        if execution_error:
            return await self._verify_with_code(results, sql, execution_error)
        if self.settings.use_llm_verification:
            return await self._verify_with_llm(results, sql, question)
        return await self._verify_with_code(results, sql)

    async def _verify_with_code(
        self,
        results: list[dict[str, Any]],
        sql: str,
        execution_error: str | None = None,
    ) -> VerificationResult:
        """Verify results using code-based validation."""
        try:
            # Check 1: SQL execution error
            if execution_error:
                logger.warning("SQL execution error detected: %s", execution_error)
                suggestion = (
                    f"The SQL query failed with error: {execution_error}. "
                    "Common fixes: "
                    "1) Use CAST(column AS BIGINT) inside SUM() for monetary/count columns, "
                    "2) Check column names and table references, "
                    "3) Verify T-SQL syntax (use TOP N instead of LIMIT N)"
                )
                return VerificationResult(
                    passed=False,
                    issues=[f"SQL execution error: {execution_error}"],
                    suggestion=suggestion,
                    summary=f"Error de ejecución SQL: {execution_error[:100]}",
                )

            # Check 2: Empty results
            if not results:
                logger.warning("No results returned from query")
                return VerificationResult(
                    passed=False,
                    issues=["Query returned 0 rows - the filter values are probably wrong"],
                    suggestion=(
                        "The query returned no results. You MUST call get_distinct_values "
                        "on every categorical column you filter on to find the EXACT values "
                        "stored in the database. Do NOT guess filter values."
                    ),
                    summary="La consulta no devolvió resultados",
                )

            # Check 3: All-null columns (indicates bad JOIN or wrong filter)
            all_null_columns = [
                col for col in results[0]
                if all(row.get(col) is None for row in results)
            ]
            if all_null_columns:
                col_list = ", ".join(sorted(all_null_columns))
                logger.warning("All-null columns detected: %s", col_list)
                return VerificationResult(
                    passed=False,
                    issues=[
                        f"Columns with ALL null values: {col_list}. "
                        "This usually means a JOIN produced no matches. "
                        "Remove these columns or fix the JOIN condition."
                    ],
                    suggestion=(
                        f"The columns [{col_list}] are entirely null across all {len(results)} rows. "
                        "This indicates the JOIN that provides these columns is not matching any rows. "
                        "Regenerate the query using only columns from ONE fact table, "
                        "do NOT JOIN fact tables with each other."
                    ),
                    summary=f"Columnas completamente nulas detectadas: {col_list}",
                )

            # Check 4: Missing entities — SQL expects N entities but results have fewer
            in_matches = re.findall(
                r"NOMBRE_ENTIDAD\s+IN\s*\(([^)]+)\)", sql, re.IGNORECASE
            )
            if in_matches:
                expected_names = set(re.findall(r"'([^']+)'", in_matches[0]))
                entity_col = next(
                    (c for c in results[0] if c.upper() == "NOMBRE_ENTIDAD"),
                    None,
                )
                if entity_col and len(expected_names) > 1:
                    actual_names = {row[entity_col] for row in results if row.get(entity_col)}
                    missing = expected_names - actual_names
                    if missing:
                        missing_str = ", ".join(sorted(missing))
                        logger.warning("Missing entities in results: %s", missing_str)
                        return VerificationResult(
                            passed=False,
                            issues=[
                                f"Expected {len(expected_names)} entities but only "
                                f"{len(actual_names)} returned data. Missing: {missing_str}. "
                                "Call get_distinct_values on NOMBRE_ENTIDAD to get the EXACT names."
                            ],
                            suggestion=(
                                f"Entity names returned 0 rows: {missing_str}. "
                                "Call get_distinct_values('NOMBRE_ENTIDAD') — names are "
                                "inconsistent (some with 'S.A.', some without)."
                            ),
                            summary=f"Entidades faltantes: {missing_str}",
                        )

            # Check 5: Result set too large
            MAX_RESULT_SET_SIZE = 10_000
            if len(results) > MAX_RESULT_SET_SIZE:
                logger.warning("Very large result set: %s rows", len(results))
                return VerificationResult(
                    passed=False,
                    issues=[f"Result set too large: {len(results)} rows"],
                    suggestion="Add more specific filters or use SELECT TOP N to restrict results",
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
            logger.error("Verification error: %s", e, exc_info=True)
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

            response = await run_handler_agent(
                self.settings,
                name="ResultVerifier",
                instructions=system_prompt,
                message=user_input,
                model=self.settings.verification_agent_model,
                max_tokens=self.settings.verification_max_tokens,
                temperature=self.settings.verification_temperature,
            )

            # Parse response
            result = JSONParser.extract_json(response)
            is_valid = bool(result.get("is_valid", False))
            issues = result.get("issues", [])
            suggestion = result.get("suggestion")
            insight = result.get("insight")
            summary = result.get("summary")

            if not is_valid:
                logger.warning("LLM Verification failed: %s", issues)

            return VerificationResult(
                passed=is_valid,
                issues=issues if isinstance(issues, list) else [str(issues)],
                suggestion=suggestion,
                insight=insight,
                summary=summary,
            )

        except Exception as e:
            logger.error("LLM verification error: %s", e, exc_info=True)
            # Fallback to code-based verification on error
            return await self._verify_with_code(results, sql)
