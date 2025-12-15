"""Main pipeline orchestrator."""

import json
import logging
import time
import unicodedata
from collections.abc import AsyncGenerator
from typing import Any

from src.config.archetypes import get_archetype_name
from src.config.constants import PipelineStep, PipelineStepDescription
from src.config.prompts import (
    build_format_prompt,
    build_intent_system_prompt,
    build_sql_execution_system_prompt,
    build_sql_generation_system_prompt,
    build_triage_system_prompt,
    build_verification_system_prompt,
    build_viz_prompt,
)
from src.config.settings import Settings
from src.infrastructure.logging.session_logger import SessionLogger
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

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the complete NL2SQL pipeline."""

    def __init__(self, settings: Settings):
        """Initialize orchestrator with settings."""
        self.settings = settings
        self.triage = TriageClassifier(settings)
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

    async def _step_triage(self, state: PipelineState, message: str) -> dict[str, Any]:
        """Execute triage step."""
        logger.info(f"{PipelineStep.TRIAGE.value}: {PipelineStepDescription.TRIAGE.value}")
        triage_prompt = build_triage_system_prompt()
        start_time = time.time()
        triage_result = await self.triage.classify(message)
        execution_time = (time.time() - start_time) * 1000

        if not triage_result or "query_type" not in triage_result:
            logger.error(
                f"TriageClassifier returned invalid result: {triage_result}. "
                "Defaulting to data_question."
            )
            return {
                "patron": "NA",
                "datos": [{"NA": {}}],
                "arquetipo": "NA",
                "visualizacion": "NA",
                "tipo_grafica": "NA",
                "imagen": "NA",
                "link_power_bi": "NA",
                "insight": "",
                "error": "Error parsing triage result, defaulting to data_question",
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

        if state.query_type != "data_question":
            response = self._format_non_data_response(state, triage_result)
            self.session_logger.end_session(
                success=True,
                final_message=json.dumps(response, indent=2, ensure_ascii=False),
                errors=[],
            )
            return response

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

    async def _step_schema(self, state: PipelineState, message: str) -> dict[str, Any]:
        """Execute schema selection step."""
        logger.info(f"{PipelineStep.SCHEMA.value}: {PipelineStepDescription.SCHEMA.value}")
        start_time = time.time()
        schema_result = await self.schema.get_schema_context(message)
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
        self, state: PipelineState, message: str, max_retries: int = 2
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

    async def _step_sql_execution(self, state: PipelineState) -> dict[str, Any]:
        """Execute SQL query step."""
        logger.info(
            f"{PipelineStep.SQL_EXECUTION.value}: {PipelineStepDescription.SQL_EXECUTION.value}"
        )
        sql_exec_prompt = build_sql_execution_system_prompt()
        if state.sql_query is None:
            raise ValueError("SQL query is not set for execution")
        start_time = time.time()
        exec_result = await self.sql_exec.execute(state.sql_query)
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
        """Execute verification step."""
        logger.info(
            f"{PipelineStep.VERIFICATION.value}: {PipelineStepDescription.VERIFICATION.value}"
        )
        verification_prompt = (
            build_verification_system_prompt() if self.settings.use_llm_verification else None
        )
        start_time = time.time()
        results_for_verification: list[dict[str, Any]] = state.sql_results or []
        sql_for_verification: str = state.sql_query or ""
        state.verification_passed = await self.verifier.verify(
            results_for_verification, sql_for_verification, message
        )
        execution_time = (time.time() - start_time) * 1000
        verification_result = {"passed": state.verification_passed}
        self.session_logger.log_agent_response(
            agent_name="ResultVerifier",
            raw_response=json.dumps(verification_result, indent=2, ensure_ascii=False),
            parsed_response=verification_result,
            input_text=(f"SQL: {state.sql_query}\nResults: {len(state.sql_results or [])} rows"),
            system_prompt=verification_prompt,
            execution_time_ms=execution_time,
        )
        return verification_result

    async def _step_visualization(
        self, state: PipelineState, message: str
    ) -> dict[str, Any] | None:
        """Execute visualization step."""
        if not (state.viz_required and state.sql_results):
            return None

        logger.info(f"{PipelineStep.VIZ.value}: {PipelineStepDescription.VIZ.value}")
        viz_prompt = build_viz_prompt()
        start_time = time.time()

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
        }

        viz_result = await self.viz.generate(
            state.sql_results,
            state.user_id,
            message,
            sql_query=state.sql_query,
            tablas=state.selected_tables,
            resumen=state.sql_resumen,
        )
        execution_time = (time.time() - start_time) * 1000
        state.tipo_grafico = viz_result.get("tipo_grafico")
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
            chart_type = str(viz_result.get("tipo_grafico"))
            graph_result = await self.graph.generate(
                run_id=run_id,
                chart_type=chart_type,
                data_points=viz_result.get("data_points", []),
                title=state.user_message,
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
        errors = []

        # Start session logging
        self.session_logger.start_session(user_id=user_id, user_message=message)

        try:
            # Step 1: TRIAGE
            triage_result = await self._step_triage(state, message)
            if state.query_type != "data_question":
                return triage_result

            # Step 2: INTENT
            intent_result = await self._step_intent(state, message)
            if state.pattern_type != "comparacion":
                return intent_result

            # Step 3: SCHEMA
            await self._step_schema(state, message)

            # Step 4: SQL_GENERATION (includes validation)
            sql_result = await self._step_sql_generation(state, message, max_retries=2)
            if sql_result.get("error"):
                errors.append(sql_result.get("error", ""))
                self.session_logger.end_session(
                    success=False,
                    final_message=json.dumps(sql_result, indent=2, ensure_ascii=False),
                    errors=errors,
                )
                return sql_result

            # Step 5: SQL_EXECUTION
            await self._step_sql_execution(state)

            # Step 6: VERIFICATION
            await self._step_verification(state, message)

            # Step 7: VISUALIZATION
            viz_result = await self._step_visualization(state, message)

            # Step 8: GRAPH
            if viz_result:
                graph_result = await self._step_graph(state, viz_result)
                if graph_result and "error" in graph_result:
                    errors.append(graph_result["error"])

            # Step 9: FORMAT
            final_response = await self._step_format(state)

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
        errors = []

        # Start session logging
        self.session_logger.start_session(user_id=user_id, user_message=message)

        try:
            # Step 1: TRIAGE
            triage_result = await self._step_triage(state, message)
            yield {
                "step": "triage",
                "result": triage_result,
                "state": {"query_type": state.query_type},
            }
            if state.query_type != "data_question":
                yield {"step": "complete", "response": triage_result}
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
            schema_result = await self._step_schema(state, message)
            yield {
                "step": "schema",
                "result": schema_result,
                "state": {"selected_tables": state.selected_tables},
            }

            # Step 4: SQL_GENERATION (includes validation)
            sql_result = await self._step_sql_generation(state, message, max_retries=2)
            yield {
                "step": "sql_generation",
                "result": sql_result,
                "state": {"sql_query": state.sql_query},
            }
            if sql_result.get("error"):
                errors.append(sql_result.get("error", ""))
                self.session_logger.end_session(
                    success=False,
                    final_message=json.dumps(sql_result, indent=2, ensure_ascii=False),
                    errors=errors,
                )
                yield {"step": "complete", "response": sql_result}
                return

            # Step 5: SQL_EXECUTION
            exec_result = await self._step_sql_execution(state)
            yield {
                "step": "sql_execution",
                "result": exec_result,
                "state": {
                    "total_filas": state.total_filas,
                    "sql_resumen": state.sql_resumen,
                },
            }

            # Step 6: VERIFICATION
            verification_result = await self._step_verification(state, message)
            yield {
                "step": "verification",
                "result": verification_result,
                "state": {"verification_passed": state.verification_passed},
            }

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

    def _format_non_data_response(
        self, state: PipelineState, triage_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Format response for non-data questions."""
        query_type = state.query_type
        reasoning = triage_result.get("reasoning", "This is not a data question.")

        # Special format for out_of_scope queries
        if query_type == "out_of_scope":
            return {
                "patron": "NA",
                "datos": [{"NA": {}}],
                "arquetipo": "NA",
                "visualizacion": "NA",
                "tipo_grafica": "NA",
                "imagen": "NA",
                "link_power_bi": "NA",
                "insight": "",
                "error": reasoning,
            }

        return {
            "patron": "NA",
            "datos": [],
            "arquetipo": "NA",
            "visualizacion": "NA",
            "tipo_grafica": "NA",
            "imagen": "NA",
            "link_power_bi": "NA",
            "insight": "",
            "error": reasoning,
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
