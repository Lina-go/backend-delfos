"""SQL generation, validation, execution and verification flow with retry."""

import json
import logging
import time
from collections.abc import AsyncGenerator
from typing import Any

from src.api.response import build_response
from src.config.constants import PipelineStep, log_pipeline_step
from src.config.prompts import (
    build_sql_execution_system_prompt,
    build_sql_generation_system_prompt,
    build_sql_retry_user_input,
    build_verification_system_prompt,
)
from src.config.settings import Settings
from src.infrastructure.database import DelfosTools
from src.infrastructure.logging.session_logger import SessionLogger
from src.orchestrator.state import PipelineState
from src.orchestrator.step_timer import timed_step
from src.services.sql.executor import SQLExecutor
from src.services.sql.generator import SQLGenerator
from src.services.sql.validation import SQLValidationService
from src.services.verification.verification_result import VerificationResult
from src.services.verification.verifier import ResultVerifier

logger = logging.getLogger(__name__)


class SQLFlowOrchestrator:
    """Orchestrate the SQL generation, execution and verification cycle.

    Encapsulates retry logic for both SQL validation (inside generation) and
    post-execution verification so that callers (sync *process* and streaming
    *process_stream*) do not need to duplicate it.
    """

    def __init__(
        self,
        settings: Settings,
        sql_gen: SQLGenerator,
        sql_validation: SQLValidationService,
        sql_exec: SQLExecutor,
        verifier: ResultVerifier,
        session_logger: SessionLogger,
    ) -> None:
        self.settings = settings
        self.sql_gen = sql_gen
        self.sql_validation = sql_validation
        self.sql_exec = sql_exec
        self.verifier = verifier
        self.session_logger = session_logger

    async def execute(
        self,
        state: PipelineState,
        message: str,
        *,
        db_tools: DelfosTools | None = None,
    ) -> dict[str, Any] | None:
        """Run SQL gen → exec → verify with retries.  Return error dict or None on success."""
        async for event in self._run(state, message, db_tools=db_tools):
            if event.get("step") == "sql_generation" and event.get("result", {}).get("error"):
                result: dict[str, Any] = event["result"]
                return result
        return None

    async def execute_streaming(
        self,
        state: PipelineState,
        message: str,
        *,
        db_tools: DelfosTools | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Same as *execute* but yields step events for SSE streaming."""
        async for event in self._run(state, message, db_tools=db_tools):
            yield event

    async def _run(
        self,
        state: PipelineState,
        message: str,
        *,
        db_tools: DelfosTools | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        max_verification_retries = self.settings.sql_max_verification_retries
        verification_attempt = 0

        while verification_attempt < max_verification_retries:
            retry_message = message
            if verification_attempt > 0 and state.verification_issues:
                retry_message = build_sql_retry_user_input(
                    original_question=message,
                    previous_sql=state.sql_query or "",
                    verification_issues=state.verification_issues,
                    verification_suggestion=state.verification_suggestion,
                )
                logger.info(
                    "Verification retry attempt %d/%d",
                    verification_attempt + 1,
                    max_verification_retries,
                )
                state.reset_sql_state()

            # Step 4: SQL GENERATION (includes validation retries internally)
            sql_result = await self._step_sql_generation(
                state,
                retry_message if verification_attempt > 0 else message,
                max_retries=self.settings.sql_max_retries,
                db_tools=db_tools,
            )
            yield {
                "step": "sql_generation",
                "result": sql_result,
                "state": {"sql_query": state.sql_query},
                "verification_attempt": verification_attempt + 1,
            }

            if sql_result.get("error"):
                return  # SQL generation failed

            # Step 5: SQL EXECUTION
            exec_result = await self._step_sql_execution(state, db_tools=db_tools)
            yield {
                "step": "sql_execution",
                "result": exec_result,
                "state": {
                    "total_filas": state.total_filas,
                    "sql_resumen": state.sql_resumen,
                },
                "verification_attempt": verification_attempt + 1,
            }

            # Step 6: VERIFICATION
            verification_result = await self._step_verification(state, message)
            yield {
                "step": "verification",
                "result": verification_result,
                "state": {
                    "verification_passed": state.verification_passed,
                    "verification_issues": state.verification_issues,
                },
                "verification_attempt": verification_attempt + 1,
            }

            if state.verification_passed:
                logger.info("Verification passed on attempt %d", verification_attempt + 1)
                return

            verification_attempt += 1
            if verification_attempt < max_verification_retries:
                logger.warning(
                    "Verification failed (attempt %d/%d). Issues: %s. Retrying...",
                    verification_attempt,
                    max_verification_retries,
                    state.verification_issues,
                )
            else:
                logger.warning(
                    "Verification failed after %d attempts. Final issues: %s",
                    max_verification_retries,
                    state.verification_issues,
                )

    async def _step_sql_generation(
        self,
        state: PipelineState,
        message: str,
        max_retries: int = 2,
        db_tools: DelfosTools | None = None,
    ) -> dict[str, Any]:
        """SQL generation with validation retry loop."""
        log_pipeline_step(PipelineStep.SQL_GENERATION)
        sql_result: dict[str, Any] = {}
        validation_errors: list[str] | None = None
        previous_sql: str | None = None

        for attempt in range(max_retries):
            prioritized_tables = (
                state.schema_context.get("tables", []) if state.schema_context else None
            )
            sql_prompt = build_sql_generation_system_prompt(
                prioritized_tables=prioritized_tables,
                temporality=state.temporality,
            )
            start_time = time.time()
            sql_result = await self.sql_gen.generate(
                message=message,
                schema_context=state.schema_context,
                intent=state.intent,
                pattern_type=state.pattern_type,
                arquetipo=state.arquetipo,
                temporality=state.temporality,
                previous_errors=validation_errors,
                previous_sql=previous_sql,
                db_tools=db_tools,
            )
            execution_time = (time.time() - start_time) * 1000
            state.sql_query = sql_result.get("sql")
            state.sql_tables = sql_result.get("tablas", [])

            sql_input = {
                "message": message,
                "schema_context": state.schema_context,
                "intent": state.intent,
                "pattern_type": state.pattern_type,
                "arquetipo": state.arquetipo,
                "temporality": state.temporality,
                "previous_errors": validation_errors,
            }
            self.session_logger.log_agent_response(
                agent_name=f"SQLGenerator_attempt_{attempt + 1}",
                raw_response=json.dumps(sql_result, indent=2, ensure_ascii=False),
                parsed_response=sql_result,
                input_text=json.dumps(sql_input, indent=2, ensure_ascii=False),
                system_prompt=sql_prompt,
                execution_time_ms=execution_time,
            )

            sql_error = sql_result.get("error")
            if sql_error and not state.sql_query:
                logger.warning("SQLGenerator could not generate query: %s", sql_error)
                return build_response(
                    patron=state.pattern_type or "error",
                    arquetipo=state.arquetipo,
                    titulo_grafica=state.titulo_grafica,
                    error=sql_error,
                )

            # Validation
            log_pipeline_step(PipelineStep.SQL_VALIDATION)
            if state.sql_query is None:
                return build_response(
                    patron="error",
                    arquetipo=state.arquetipo,
                    titulo_grafica=state.titulo_grafica,
                    error="SQL validation failed: empty SQL query",
                )
            start_time = time.time()
            validation_result = self.sql_validation.validate(state.sql_query)
            execution_time = (time.time() - start_time) * 1000

            self.session_logger.log_agent_response(
                agent_name="SQLValidation",
                raw_response=json.dumps(validation_result, indent=2, ensure_ascii=False),
                parsed_response=validation_result,
                input_text=state.sql_query,
                execution_time_ms=execution_time,
            )

            if validation_result["is_valid"]:
                break

            validation_errors = validation_result["errors"]
            previous_sql = state.sql_query
            logger.warning(
                "SQL validation failed (attempt %d/%d): %s",
                attempt + 1,
                max_retries,
                validation_errors,
            )

            if attempt < max_retries - 1:
                logger.info("Retrying SQL generation with validation error feedback...")
            else:
                logger.error(
                    "SQL validation failed after %d attempts: %s",
                    max_retries,
                    validation_errors,
                )
                return build_response(
                    patron="error",
                    arquetipo=state.arquetipo,
                    titulo_grafica=state.titulo_grafica,
                    error=f"SQL validation failed after {max_retries} attempts: {', '.join(validation_errors)}",
                )

        return sql_result

    async def _step_sql_execution(
        self,
        state: PipelineState,
        db_tools: DelfosTools | None = None,
    ) -> dict[str, Any]:
        """Execute the SQL query."""
        if state.sql_query is None:
            raise ValueError("SQL query is not set for execution")
        sql_exec_prompt = build_sql_execution_system_prompt()
        async with timed_step(
            PipelineStep.SQL_EXECUTION, self.session_logger, "SQLExecutor",
            input_text=state.sql_query, system_prompt=sql_exec_prompt,
        ) as ctx:
            exec_result = await self.sql_exec.execute(state.sql_query, db_tools=db_tools)
            state.sql_results = exec_result.get("resultados", [])
            state.total_filas = exec_result.get("total_filas", 0)
            state.sql_resumen = exec_result.get("resumen")
            state.sql_insights = exec_result.get("insights")
            ctx.set_result(exec_result)
        return exec_result

    async def _step_verification(self, state: PipelineState, message: str) -> dict[str, Any]:
        """Verify SQL results."""
        verification_prompt = (
            build_verification_system_prompt() if self.settings.use_llm_verification else None
        )
        verify_input = f"SQL: {state.sql_query}\nResults: {len(state.sql_results or [])} rows"

        async with timed_step(
            PipelineStep.VERIFICATION, self.session_logger, "ResultVerifier",
            input_text=verify_input, system_prompt=verification_prompt,
        ) as ctx:
            results_for_verification: list[dict[str, Any]] = state.sql_results or []
            sql_for_verification: str = state.sql_query or ""

            execution_error: str | None = None
            resumen = state.sql_resumen or ""
            if resumen.startswith("Error executing SQL:"):
                execution_error = resumen.removeprefix("Error executing SQL: ").strip()

            verification_result: VerificationResult = await self.verifier.verify(
                results_for_verification, sql_for_verification, message,
                execution_error=execution_error,
            )

            state.verification_passed = verification_result.passed
            state.verification_issues = verification_result.issues
            state.verification_suggestion = verification_result.suggestion
            state.verification_insight = verification_result.insight

            result_dict = verification_result.to_dict()
            ctx.set_result(result_dict)

        return result_dict
