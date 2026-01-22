"""Main pipeline orchestrator."""

import json
import logging
import time
import unicodedata
from collections.abc import AsyncGenerator
from typing import Any

from src.config.archetypes import (
    get_archetype_name,
    get_chart_type_for_archetype,
    get_archetype_letter_by_name,
)
from src.config.constants import PipelineStep, PipelineStepDescription, QueryType
from src.config.prompts import (
    build_format_prompt,
    build_intent_system_prompt,
    build_sql_execution_system_prompt,
    build_sql_generation_system_prompt,
    build_sql_retry_user_input,
    build_triage_system_prompt,
    build_verification_system_prompt,
    build_viz_prompt,
)
from src.config.settings import Settings
from src.infrastructure.logging.session_logger import SessionLogger
from src.infrastructure.mcp.client import mcp_connection
from src.orchestrator.state import PipelineState
from src.services.formatting.formatter import ResponseFormatter
from src.services.graph.service import GraphService
from src.services.intent.classifier import IntentClassifier
from src.services.schema.service import SchemaService
from src.services.sql.executor import SQLExecutor
from src.services.sql.generator import SQLGenerator
from src.services.sql.validation import SQLValidationService
from src.services.triage.classifier import TriageClassifier
from src.services.verification.verifier import ResultVerifier
from src.services.viz.service import VisualizationService
from src.config.message import get_rejection_message
from src.orchestrator.context import ConversationStore
from src.orchestrator.handlers import GreetingHandler, FollowUpHandler, VizRequestHandler, GeneralHandler
from src.services.verification.verification_result import VerificationResult

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the complete NL2SQL pipeline."""

    def __init__(self, settings: Settings):
        """Initialize orchestrator with settings."""
        self.settings = settings
        self.triage = TriageClassifier(settings)
        self.greeting_handler = GreetingHandler()
        self.follow_up_handler = FollowUpHandler(settings)
        self.viz_request_handler = VizRequestHandler(settings)
        self.general_handler = GeneralHandler(settings)
        self.intent = IntentClassifier(settings)
        self.schema = SchemaService(settings)
        self.sql_gen = SQLGenerator(settings)
        self.sql_validation = SQLValidationService()
        self.sql_exec = SQLExecutor(settings)
        self.verifier = ResultVerifier(settings)
        self.viz = VisualizationService(settings)
        self.graph = GraphService(settings)
        self.formatter = ResponseFormatter(settings)
        self.session_logger = SessionLogger()

    async def close(self) -> None:
        """Close all service connections and cleanup resources."""
        try:
            # Close MCP clients
            if hasattr(self.schema, "close"):
                await self.schema.close()
            if hasattr(self.sql_exec, "close"):
                await self.sql_exec.close()
            logger.info("Pipeline resources closed")
        except Exception as e:
            logger.error(f"Error closing pipeline resources: {e}", exc_info=True)

    async def _step_triage(
        self,
        state: PipelineState,
        message: str,
        has_context: bool = False,
        context_summary: str | None = None,
        mcp: Any | None = None,
    ) -> dict[str, Any]:
        """Execute triage step with context awareness.

        Args:
            state: Current pipeline state
            message: User's natural language question
            has_context: Whether the user has previous conversation data
            context_summary: Summary of available data in context

        Returns:
            Triage classification result
        """
        logger.info(f"{PipelineStep.TRIAGE.value}: {PipelineStepDescription.TRIAGE.value}")
        triage_prompt = build_triage_system_prompt(
            has_context=has_context,
            context_summary=context_summary,
        )
        start_time = time.time()
        triage_result = await self.triage.classify(
            message,
            has_context=has_context,
            context_summary=context_summary,
            mcp=mcp,
        )
        execution_time = (time.time() - start_time) * 1000

        if not triage_result or "query_type" not in triage_result:
            logger.error(
                f"TriageClassifier returned invalid result: {triage_result}. "
                "Defaulting to data_question."
            )
            state.query_type = "data_question"
            return {
                "query_type": "data_question",
                "reasoning": "Error parsing triage result, defaulting to data_question",
            }

        state.query_type = triage_result["query_type"]
        self.session_logger.log_agent_response(
            agent_name="TriageClassifier",
            raw_response=json.dumps(triage_result, indent=2, ensure_ascii=False),
            parsed_response=triage_result,
            input_text=message,
            system_prompt=triage_prompt,
            execution_time_ms=execution_time,
        )

        return triage_result

    async def _step_intent(self, state: PipelineState, message: str) -> dict[str, Any]:
        """Execute intent classification step."""
        logger.info(f"{PipelineStep.INTENT.value}: {PipelineStepDescription.INTENT.value}")
        intent_prompt = build_intent_system_prompt()
        start_time = time.time()
        intent_result = await self.intent.classify(message)
        execution_time = (time.time() - start_time) * 1000

        state.intent = intent_result["intent"]
        raw_pattern = intent_result.get("tipo_patron", "")
        state.pattern_type = (
            unicodedata.normalize("NFKD", raw_pattern).encode("ascii", "ignore").decode().lower()
        )
        archetype_letter = str(intent_result.get("arquetipo", "N"))
        state.arquetipo = get_archetype_name(archetype_letter)
        state.viz_required = state.intent == "requiere_visualizacion"
        self.session_logger.log_agent_response(
            agent_name="IntentClassifier",
            raw_response=json.dumps(intent_result, indent=2, ensure_ascii=False),
            parsed_response=intent_result,
            input_text=message,
            system_prompt=intent_prompt,
            execution_time_ms=execution_time,
        )

        if state.pattern_type != "comparacion":
            response = self._format_non_comparacion_response(state, intent_result)
            self.session_logger.end_session(
                success=True,
                final_message=json.dumps(response, indent=2, ensure_ascii=False),
                errors=[],
            )
            return response

        return intent_result

    async def _step_schema(
        self, state: PipelineState, message: str, mcp: Any | None = None
    ) -> dict[str, Any]:
        """Execute schema selection step."""
        logger.info(f"{PipelineStep.SCHEMA.value}: {PipelineStepDescription.SCHEMA.value}")
        start_time = time.time()
        schema_result = await self.schema.get_schema_context(message, mcp=mcp)
        execution_time = (time.time() - start_time) * 1000
        state.selected_tables = schema_result.get("tables", [])
        state.schema_context = schema_result
        self.session_logger.log_agent_response(
            agent_name="SchemaService",
            raw_response=json.dumps(schema_result, indent=2, ensure_ascii=False),
            parsed_response=schema_result,
            input_text=message,
            execution_time_ms=execution_time,
        )
        return schema_result

    async def _step_sql_generation(
        self,
        state: PipelineState,
        message: str,
        max_retries: int = 2,
        mcp: Any | None = None,
    ) -> dict[str, Any]:
        """Execute SQL generation step with validation and retries."""
        logger.info(
            f"{PipelineStep.SQL_GENERATION.value}: {PipelineStepDescription.SQL_GENERATION.value}"
        )
        sql_result: dict[str, Any] = {}
        validation_errors: list[str] | None = None
        previous_sql: str | None = None

        for attempt in range(max_retries):
            prioritized_tables = (
                state.schema_context.get("tables", []) if state.schema_context else None
            )
            sql_prompt = build_sql_generation_system_prompt(prioritized_tables=prioritized_tables)
            start_time = time.time()
            sql_result = await self.sql_gen.generate(
                message=message,
                schema_context=state.schema_context,
                intent=state.intent,
                pattern_type=state.pattern_type,
                arquetipo=state.arquetipo,
                previous_errors=validation_errors,
                previous_sql=previous_sql,
                mcp=mcp,
            )
            execution_time = (time.time() - start_time) * 1000
            state.sql_query = sql_result.get("sql")

            sql_input = {
                "message": message,
                "schema_context": state.schema_context,
                "intent": state.intent,
                "pattern_type": state.pattern_type,
                "arquetipo": state.arquetipo,
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
                logger.warning(f"SQLGenerator could not generate query: {sql_error}")
                return {
                    "patron": state.pattern_type,
                    "datos": [],
                    "arquetipo": state.arquetipo,
                    "visualizacion": "NO",
                    "tipo_grafica": None,
                    "imagen": None,
                    "link_power_bi": None,
                    "insight": "",
                    "error": sql_error,
                }

            # SQL Validation
            logger.info(
                f"{PipelineStep.SQL_VALIDATION.value}: {PipelineStepDescription.SQL_VALIDATION.value}"
            )
            if state.sql_query is None:
                return {
                    "patron": "error",
                    "datos": [],
                    "arquetipo": state.arquetipo,
                    "visualizacion": "NO",
                    "tipo_grafica": None,
                    "imagen": None,
                    "link_power_bi": None,
                    "insight": "",
                    "error": "SQL validation failed: empty SQL query",
                }
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
            else:
                validation_errors = validation_result["errors"]
                previous_sql = state.sql_query
                logger.warning(
                    f"SQL validation failed (attempt {attempt + 1}/{max_retries}): {validation_errors}"
                )

                if attempt < max_retries - 1:
                    logger.info("Retrying SQL generation with validation error feedback...")
                else:
                    logger.error(
                        f"SQL validation failed after {max_retries} attempts: {validation_errors}"
                    )
                    return {
                        "patron": "error",
                        "datos": [],
                        "arquetipo": state.arquetipo,
                        "visualizacion": "NO",
                        "tipo_grafica": None,
                        "imagen": None,
                        "link_power_bi": None,
                        "insight": "",
                        "error": f"SQL validation failed after {max_retries} attempts: {', '.join(validation_errors)}",
                    }

        return sql_result

    async def _step_sql_execution(
        self, state: PipelineState, mcp: Any | None = None
    ) -> dict[str, Any]:
        """Execute SQL query step."""
        logger.info(
            f"{PipelineStep.SQL_EXECUTION.value}: {PipelineStepDescription.SQL_EXECUTION.value}"
        )
        sql_exec_prompt = build_sql_execution_system_prompt()
        if state.sql_query is None:
            raise ValueError("SQL query is not set for execution")
        start_time = time.time()
        exec_result = await self.sql_exec.execute(state.sql_query, mcp=mcp)
        execution_time = (time.time() - start_time) * 1000
        state.sql_results = exec_result.get("resultados", [])
        state.total_filas = exec_result.get("total_filas", 0)
        state.sql_resumen = exec_result.get("resumen")
        state.sql_insights = exec_result.get("insights")
        self.session_logger.log_agent_response(
            agent_name="SQLExecutor",
            raw_response=json.dumps(exec_result, indent=2, ensure_ascii=False),
            parsed_response=exec_result,
            input_text=state.sql_query,
            system_prompt=sql_exec_prompt,
            execution_time_ms=execution_time,
        )
        return exec_result

    async def _step_verification(self, state: PipelineState, message: str) -> dict[str, Any]:
        """Execute verification step with detailed feedback."""
        logger.info(
            f"{PipelineStep.VERIFICATION.value}: {PipelineStepDescription.VERIFICATION.value}"
        )
        verification_prompt = (
            build_verification_system_prompt() if self.settings.use_llm_verification else None
        )
        start_time = time.time()
        
        results_for_verification: list[dict[str, Any]] = state.sql_results or []
        sql_for_verification: str = state.sql_query or ""
        
        # verify now returns VerificationResult instead of bool
        verification_result: VerificationResult = await self.verifier.verify(
            results_for_verification, sql_for_verification, message
        )
        
        execution_time = (time.time() - start_time) * 1000
        
        # Store detailed feedback in state for retry logic
        state.verification_passed = verification_result.passed
        state.verification_issues = verification_result.issues
        state.verification_suggestion = verification_result.suggestion
        state.verification_insight = verification_result.insight
        
        result_dict = verification_result.to_dict()
        
        self.session_logger.log_agent_response(
            agent_name="ResultVerifier",
            raw_response=json.dumps(result_dict, indent=2, ensure_ascii=False),
            parsed_response=result_dict,
            input_text=(f"SQL: {state.sql_query}\nResults: {len(state.sql_results or [])} rows"),
            system_prompt=verification_prompt,
            execution_time_ms=execution_time,
        )
        
        return result_dict
    
    async def _step_sql_with_verification_retry(
        self,
        state: PipelineState,
        message: str,
        max_verification_retries: int = 2,
        mcp: Any | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Execute SQL generation -> execution -> verification with retry loop.
        
        If verification fails (e.g., 0 results), retries SQL generation with feedback.
        
        Args:
            state: Pipeline state
            message: User's original message
            max_verification_retries: Max times to retry after verification failure
            
        Yields:
            Step events for streaming
        """
        verification_attempt = 0
        
        while verification_attempt < max_verification_retries:
            # Build retry context if this is a retry attempt
            retry_message = message
            if verification_attempt > 0 and state.verification_issues:
                retry_message = build_sql_retry_user_input(
                    original_question=message,
                    previous_sql=state.sql_query or "",
                    verification_issues=state.verification_issues,
                    verification_suggestion=state.verification_suggestion,
                )
                logger.info(
                    f"Verification retry attempt {verification_attempt + 1}/{max_verification_retries}"
                )
                
                # Reset SQL state for retry
                state.reset_sql_state()
            
            # Step 4: SQL GENERATION (with validation retry loop inside)
            sql_result = await self._step_sql_generation(
                state,
                retry_message if verification_attempt > 0 else message,
                max_retries=2,
                mcp=mcp,
            )
            yield {
                "step": "sql_generation",
                "result": sql_result,
                "state": {"sql_query": state.sql_query},
                "verification_attempt": verification_attempt + 1,
            }
            
            if sql_result.get("error"):
                # SQL generation failed, exit retry loop
                return
            
            # Step 5: SQL EXECUTION
            exec_result = await self._step_sql_execution(state, mcp=mcp)
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
            
            # Check if verification passed
            if state.verification_passed:
                logger.info(
                    f"Verification passed on attempt {verification_attempt + 1}"
                )
                return
            
            # Verification failed - decide whether to retry
            verification_attempt += 1
            
            if verification_attempt < max_verification_retries:
                logger.warning(
                    f"Verification failed (attempt {verification_attempt}/{max_verification_retries}). "
                    f"Issues: {state.verification_issues}. Retrying..."
                )
            else:
                logger.warning(
                    f"Verification failed after {max_verification_retries} attempts. "
                    f"Final issues: {state.verification_issues}"
                )

    async def _step_visualization(
        self, state: PipelineState, message: str
    ) -> dict[str, Any] | None:
        """Execute visualization step."""
        if not (state.viz_required and state.sql_results):
            return None

        logger.info(f"{PipelineStep.VIZ.value}: {PipelineStepDescription.VIZ.value}")
        viz_prompt = build_viz_prompt()
        start_time = time.time()

        state.tipo_grafico = get_chart_type_for_archetype(get_archetype_letter_by_name(state.arquetipo))
        logger.info(f"Determined chart type: {state.tipo_grafico} for archetype: {state.arquetipo}")

        viz_input = {
            "user_id": state.user_id,
            "sql_results": {
                "pregunta_original": message,
                "sql": state.sql_query or "",
                "tablas": state.selected_tables or [],
                "resultados": state.sql_results,
                "total_filas": len(state.sql_results or []),
                "resumen": state.sql_resumen or "",
            },
            "original_question": message,
            "tipo_grafico": state.tipo_grafico
        }

        viz_result = await self.viz.generate(
            state.sql_results,
            state.user_id,
            message,
            sql_query=state.sql_query,
            tablas=state.selected_tables,
            resumen=state.sql_resumen,
            chart_type=state.tipo_grafico,
        )
        execution_time = (time.time() - start_time) * 1000
        state.powerbi_url = viz_result.get("powerbi_url")
        state.image_url = viz_result.get("image_url")
        state.run_id = viz_result.get("run_id")
        self.session_logger.log_agent_response(
            agent_name="VisualizationService",
            raw_response=json.dumps(viz_result, indent=2, ensure_ascii=False),
            parsed_response=viz_result,
            input_text=json.dumps(viz_input, indent=2, ensure_ascii=False),
            system_prompt=viz_prompt,
            execution_time_ms=execution_time,
        )
        return viz_result

    async def _step_graph(
        self, state: PipelineState, viz_result: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Execute graph generation step."""
        if not (viz_result and viz_result.get("data_points") and viz_result.get("run_id")):
            return None

        logger.info(f"{PipelineStep.GRAPH.value}: {PipelineStepDescription.GRAPH.value}")
        try:
            start_time = time.time()
            run_id = str(viz_result.get("run_id"))
            chart_type = state.tipo_grafico
            title = (
                viz_result.get("metric_name")
                or state.sql_resumen
                or "Visualizacion de resultados"
            )
            graph_result = await self.graph.generate(
                run_id=run_id,
                chart_type=chart_type,
                data_points=viz_result.get("data_points", []),
                title=title,
            )
            execution_time = (time.time() - start_time) * 1000
            state.image_url = graph_result.image_url
            state.html_url = graph_result.html_url
            state.png_url = graph_result.png_url
            graph_response = {
                "image_url": graph_result.image_url,
                "html_url": graph_result.html_url,
                "png_url": graph_result.png_url,
                "html_path": graph_result.html_path,
                "png_path": graph_result.png_path,
            }
            self.session_logger.log_agent_response(
                agent_name="GraphService",
                raw_response=json.dumps(graph_response, indent=2, ensure_ascii=False),
                parsed_response=graph_response,
                input_text=(f"Run ID: {run_id}\nChart Type: {chart_type}"),
                execution_time_ms=execution_time,
            )
            return graph_response
        except Exception as e:
            logger.warning(f"Graph generation failed: {e}")
            return {"error": str(e)}

    async def _step_format(self, state: PipelineState) -> dict[str, Any]:
        """Execute response formatting step."""
        logger.info(f"{PipelineStep.FORMAT.value}: {PipelineStepDescription.FORMAT.value}")
        format_prompt = build_format_prompt() if self.settings.use_llm_formatting else None
        start_time = time.time()
        state.final_response = await self.formatter.format(state)
        execution_time = (time.time() - start_time) * 1000
        self.session_logger.log_agent_response(
            agent_name="ResponseFormatter",
            raw_response=json.dumps(state.final_response, indent=2, ensure_ascii=False),
            parsed_response=state.final_response,
            input_text=json.dumps(
                {
                    "intent": state.intent,
                    "pattern_type": state.pattern_type,
                    "arquetipo": state.arquetipo,
                    "sql_results_count": len(state.sql_results or []),
                },
                indent=2,
                ensure_ascii=False,
            ),
            system_prompt=format_prompt,
            execution_time_ms=execution_time,
        )
        return state.final_response

    async def process(self, message: str, user_id: str) -> dict[str, Any]:
        """
        Process a user message through the complete pipeline.

        Args:
            message: User's natural language question
            user_id: User identifier

        Returns:
            Formatted response dictionary
        """
        state = PipelineState(user_message=message, user_id=user_id)
        errors: list[str] = []

        self.session_logger.start_session(user_id=user_id, user_message=message)

        mcp = None
        mcp_ctx = mcp_connection(self.settings)
        try:
            mcp = await mcp_ctx.__aenter__()

            # Get context and check if we have previous data
            context = ConversationStore.get(user_id)
            has_context = context.last_results is not None and len(context.last_results) > 0

            # Generate context summary for triage (if we have data)
            context_summary = context.get_summary() if has_context else None

            if context_summary:
                logger.info(f"Context available: {len(context.last_results)} rows from previous query")

            # Step 1: TRIAGE (with context summary)
            triage_result = await self._step_triage(
                state, message, has_context, context_summary, mcp=mcp
            )

            # Route based on query_type
            if state.query_type == "greeting":
                response = self.greeting_handler.handle(message)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                return response

            elif state.query_type == "follow_up":
                response = await self.follow_up_handler.handle(message, context)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                return response

            elif state.query_type == "viz_request":
                response = await self.viz_request_handler.handle(message, user_id, context)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                return response

            elif state.query_type in ("general", "out_of_scope"):
                response = await self.general_handler.handle(message)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                return response

            elif state.query_type == "data_question":
                pass

            else:
                response = await self.general_handler.handle(message)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                return response

            # Step 2: INTENT
            intent_result = await self._step_intent(state, message)
            if state.pattern_type != "comparacion":
                return intent_result

            # Step 3: SCHEMA
            await self._step_schema(state, message, mcp=mcp)

            # Steps 4-6: SQL GENERATION + EXECUTION + VERIFICATION with retry
            max_verification_retries = 2
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
                        f"Verification retry attempt {verification_attempt + 1}/{max_verification_retries}"
                    )
                    state.reset_sql_state()
                
                # Step 4: SQL_GENERATION (includes validation)
                sql_result = await self._step_sql_generation(
                    state,
                    retry_message if verification_attempt > 0 else message,
                    max_retries=2,
                    mcp=mcp,
                )
                if sql_result.get("error"):
                    errors.append(sql_result.get("error", ""))
                    self.session_logger.end_session(
                        success=False,
                        final_message=json.dumps(sql_result, indent=2, ensure_ascii=False),
                        errors=errors,
                    )
                    return sql_result

                # Step 5: SQL_EXECUTION
                await self._step_sql_execution(state, mcp=mcp)

                # Step 6: VERIFICATION
                await self._step_verification(state, message)

                if state.verification_passed:
                    logger.info(f"Verification passed on attempt {verification_attempt + 1}")
                    break
                    
                verification_attempt += 1
                if verification_attempt < max_verification_retries:
                    logger.warning(
                        f"Verification failed (attempt {verification_attempt}/{max_verification_retries}). "
                        f"Issues: {state.verification_issues}. Retrying..."
                    )
                else:
                    logger.warning(
                        f"Verification failed after {max_verification_retries} attempts. "
                        f"Final issues: {state.verification_issues}"
                    )

            # Step 7: VISUALIZATION
            viz_result = await self._step_visualization(state, message)

            # Step 8: GRAPH
            if viz_result:
                graph_result = await self._step_graph(state, viz_result)
                if graph_result and "error" in graph_result:
                    errors.append(graph_result["error"])

            # Step 9: FORMAT
            final_response = await self._step_format(state)

            # Save context for follow-up questions
            ConversationStore.update(
                user_id=user_id,
                query=message,
                sql=state.sql_query,
                results=state.sql_results,
                response=final_response,
                chart_type=state.tipo_grafico,
                run_id=state.run_id,
                data_points=viz_result.get("data_points") if viz_result else None,
                tables=state.schema_context.get("tables", []) if state.schema_context else [],
                schema_context=state.schema_context,
            )

            self.session_logger.end_session(
                success=True,
                final_message=json.dumps(final_response, indent=2, ensure_ascii=False),
                errors=errors,
            )
            return final_response

        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            errors.append(f"Pipeline error: {str(e)}")
            self.session_logger.end_session(
                success=False,
                final_message=f"Pipeline error: {str(e)}",
                errors=errors,
            )
            raise
        finally:
            if mcp is not None:
                await mcp_ctx.__aexit__(None, None, None)


    async def process_stream(
        self, message: str, user_id: str
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Process a user message through the complete pipeline with streaming events.

        Args:
            message: User's natural language question
            user_id: User identifier

        Yields:
            Event dictionaries with step results
        """
        state = PipelineState(user_message=message, user_id=user_id)
        errors: list[str] = []

        self.session_logger.start_session(user_id=user_id, user_message=message)

        mcp = None
        mcp_ctx = mcp_connection(self.settings)
        try:
            mcp = await mcp_ctx.__aenter__()

            # Get context and check if we have previous data
            context = ConversationStore.get(user_id)
            has_context = context.last_results is not None and len(context.last_results) > 0

            # Generate context summary for triage (if we have data)
            context_summary = context.get_summary() if has_context else None

            if context_summary:
                logger.info(f"Context available: {len(context.last_results)} rows from previous query")

            # Step 1: TRIAGE (with context summary)
            triage_result = await self._step_triage(
                state, message, has_context, context_summary, mcp=mcp
            )
            yield {
                "step": "triage",
                "result": triage_result,
                "state": {"query_type": state.query_type},
            }

            # Route based on query_type
            if state.query_type == "greeting":
                response = self.greeting_handler.handle(message)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                yield {"step": "complete", "response": response}
                return

            elif state.query_type == "follow_up":
                response = await self.follow_up_handler.handle(message, context)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                yield {"step": "complete", "response": response}
                return

            elif state.query_type == "viz_request":
                response = await self.viz_request_handler.handle(message, user_id, context)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                yield {"step": "complete", "response": response}
                return

            elif state.query_type in ("general", "out_of_scope"):
                response = await self.general_handler.handle(message)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                yield {"step": "complete", "response": response}
                return

            elif state.query_type == "data_question":
                pass

            else:
                response = await self.general_handler.handle(message)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                yield {"step": "complete", "response": response}
                return

            # Step 2: INTENT
            intent_result = await self._step_intent(state, message)
            yield {
                "step": "intent",
                "result": intent_result,
                "state": {
                    "intent": state.intent,
                    "pattern_type": state.pattern_type,
                    "arquetipo": state.arquetipo,
                    "viz_required": state.viz_required,
                },
            }
            if state.pattern_type != "comparacion":
                yield {"step": "complete", "response": intent_result}
                return

            # Step 3: SCHEMA
            schema_result = await self._step_schema(state, message, mcp=mcp)
            yield {
                "step": "schema",
                "result": schema_result,
                "state": {"selected_tables": state.selected_tables},
            }

            # Steps 4-6: SQL GENERATION + EXECUTION + VERIFICATION with retry
            async for sql_event in self._step_sql_with_verification_retry(
                state, message, max_verification_retries=2, mcp=mcp
            ):
                yield sql_event
                
                # Check if SQL generation failed
                if sql_event.get("step") == "sql_generation" and sql_event.get("result", {}).get("error"):
                    errors.append(sql_event["result"].get("error", ""))
                    self.session_logger.end_session(
                        success=False,
                        final_message=json.dumps(sql_event["result"], indent=2, ensure_ascii=False),
                        errors=errors,
                    )
                    yield {"step": "complete", "response": sql_event["result"]}
                    return

            # Step 7: VISUALIZATION
            viz_result = await self._step_visualization(state, message)
            if viz_result:
                yield {
                    "step": "visualization",
                    "result": viz_result,
                    "state": {
                        "tipo_grafico": state.tipo_grafico,
                        "powerbi_url": state.powerbi_url,
                        "run_id": state.run_id,
                    },
                }

                # Step 8: GRAPH
                graph_result = await self._step_graph(state, viz_result)
                if graph_result:
                    if "error" in graph_result:
                        errors.append(graph_result["error"])
                    yield {
                        "step": "graph",
                        "result": graph_result,
                        "state": {
                            "image_url": state.image_url,
                            "html_url": state.html_url,
                            "png_url": state.png_url,
                        },
                    }

            # Step 9: FORMAT
            final_response = await self._step_format(state)
            yield {
                "step": "format",
                "result": final_response,
            }

            # Save context for follow-up questions
            ConversationStore.update(
                user_id=user_id,
                query=message,
                sql=state.sql_query,
                results=state.sql_results,
                response=final_response,
                chart_type=state.tipo_grafico,
                run_id=state.run_id,
                data_points=viz_result.get("data_points") if viz_result else None,
                tables=state.schema_context.get("tables", []) if state.schema_context else [],
                schema_context=state.schema_context,
            )

            self.session_logger.end_session(
                success=True,
                final_message=json.dumps(final_response, indent=2, ensure_ascii=False),
                errors=errors,
            )
            yield {"step": "complete", "response": final_response}

        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            errors.append(f"Pipeline error: {str(e)}")
            self.session_logger.end_session(
                success=False,
                final_message=f"Pipeline error: {str(e)}",
                errors=errors,
            )
            yield {"step": "error", "error": str(e)}
        finally:
            if mcp is not None:
                await mcp_ctx.__aexit__(None, None, None)

    def _format_non_data_response(
        self, state: PipelineState, triage_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Format response for non-data questions (general, out_of_scope)."""
        
        query_type_str = state.query_type or "general"
        try:
            query_type = QueryType(query_type_str)
        except ValueError:
            query_type = QueryType.GENERAL
            
        message = get_rejection_message(query_type)
        reasoning = triage_result.get("reasoning", message)
        
        return {
            "patron": "general",
            "datos": [],
            "arquetipo": None,
            "visualizacion": "NO",
            "tipo_grafica": None,
            "imagen": None,
            "link_power_bi": None,
            "insight": reasoning,
            "error": "",
        }

    def _format_non_comparacion_response(
        self, state: PipelineState, intent_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Format response for non-comparacion questions."""
        pattern_type = state.pattern_type
        reasoning = intent_result.get(
            "reasoning",
            "Este tipo de pregunta aun no esta soportada. Por favor, ingrese una pregunta de comparacion.",
        )
        return {
            "patron": pattern_type,
            "datos": [{"NA": {}}],
            "arquetipo": state.arquetipo,
            "visualizacion": "NA",
            "tipo_grafica": "NA",
            "imagen": "NA",
            "link_power_bi": "NA",
            "insight": "NA",
            "error": reasoning,
        }
