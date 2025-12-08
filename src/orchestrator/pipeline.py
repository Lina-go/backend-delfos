"""Main pipeline orchestrator."""

import logging
import time
import json
from typing import Dict, Any

from src.config.settings import Settings
from src.config.constants import PipelineStep, PipelineStepDescription
from src.config.prompts import (
    build_triage_system_prompt,
    build_intent_system_prompt,
    build_sql_generation_system_prompt,
    build_sql_execution_system_prompt,
    build_verification_system_prompt,
    build_viz_prompt,
    build_format_prompt,
)
from src.orchestrator.state import PipelineState
from src.services.triage.classifier import TriageClassifier
from src.services.intent.classifier import IntentClassifier
from src.services.schema.service import SchemaService
from src.services.sql.generator import SQLGenerator
from src.services.sql.validation import SQLValidationService
from src.services.sql.executor import SQLExecutor
from src.services.verification.verifier import ResultVerifier
from src.services.viz.service import VisualizationService
from src.services.graph.service import GraphService
from src.services.formatting.formatter import ResponseFormatter
from src.infrastructure.logging.session_logger import SessionLogger

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

    async def close(self):
        """Close all service connections and cleanup resources."""
        try:
            # Close MCP clients
            if hasattr(self.schema, 'close'):
                await self.schema.close()
            if hasattr(self.sql_exec, 'close'):
                await self.sql_exec.close()
            logger.info("Pipeline resources closed")
        except Exception as e:
            logger.error(f"Error closing pipeline resources: {e}", exc_info=True)

    async def process(self, message: str, user_id: str) -> Dict[str, Any]:
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
            logger.info(f"{PipelineStep.TRIAGE.value}: {PipelineStepDescription.TRIAGE.value}")
            triage_prompt = build_triage_system_prompt()
            start_time = time.time()
            triage_result = await self.triage.classify(message)
            execution_time = (time.time() - start_time) * 1000
            
            # Validate triage_result has required fields
            if not triage_result or "query_type" not in triage_result:
                logger.error(
                    f"TriageClassifier returned invalid result: {triage_result}. "
                    "Defaulting to data_question."
                )
                triage_result = {
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

            if state.query_type != "data_question":
                # Return early for non-data questions
                response = self._format_non_data_response(state, triage_result)
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(response, indent=2, ensure_ascii=False),
                    errors=errors,
                )
                return response

            # Step 2: INTENT
            logger.info(f"{PipelineStep.INTENT.value}: {PipelineStepDescription.INTENT.value}")
            intent_prompt = build_intent_system_prompt()
            start_time = time.time()
            intent_result = await self.intent.classify(message)
            execution_time = (time.time() - start_time) * 1000

            state.intent = intent_result["intent"]
            state.pattern_type = intent_result.get("tipo_patron")
            state.arquetipo = intent_result.get("arquetipo")
            state.viz_required = state.intent == "requiere_visualizacion"
            self.session_logger.log_agent_response(
                agent_name="IntentClassifier",
                raw_response=json.dumps(intent_result, indent=2, ensure_ascii=False),
                parsed_response=intent_result,
                input_text=message,
                system_prompt=intent_prompt,
                execution_time_ms=execution_time,
            )

            # Step 3: SCHEMA
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

            # Step 4: SQL_GENERATION with auto-correction loop
            logger.info(f"{PipelineStep.SQL_GENERATION.value}: {PipelineStepDescription.SQL_GENERATION.value}")
            max_sql_retries = 2
            sql_result = None
            validation_errors = None
            previous_sql = None
            
            for attempt in range(max_sql_retries):
                # Build prompt with prioritized tables
                prioritized_tables = state.schema_context.get("tables", []) if state.schema_context else None
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
                
                # Check if SQLGenerator returned an error (information not available)
                sql_error = sql_result.get("error")
                if sql_error and not state.sql_query:
                    logger.warning(f"SQLGenerator could not generate query: {sql_error}")
                    error_response = {
                        "patron": "error",
                        "datos": [],
                        "arquetipo": state.arquetipo,
                        "visualizacion": "NO",
                        "tipo_grafica": None,
                        "imagen": None,
                        "link_power_bi": None,
                        "insight": "",
                        "error": sql_error,
                    }
                    errors.append(sql_error)
                    self.session_logger.end_session(
                        success=False,
                        final_message=json.dumps(error_response, indent=2, ensure_ascii=False),
                        errors=errors,
                    )
                    return error_response
                
                # Step 5: SQL_VALIDATION
                logger.info(f"{PipelineStep.SQL_VALIDATION.value}: {PipelineStepDescription.SQL_VALIDATION.value}")
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
                    previous_sql = state.sql_query  # Save failed SQL for retry
                    logger.warning(
                        f"SQL validation failed (attempt {attempt + 1}/{max_sql_retries}): {validation_errors}"
                    )
                    
                    if attempt < max_sql_retries - 1:
                        logger.info("Retrying SQL generation with validation error feedback...")
                    else:
                        logger.error(f"SQL validation failed after {max_sql_retries} attempts: {validation_errors}")
                        error_response = {
                            "patron": "error",
                            "datos": [],
                            "arquetipo": state.arquetipo,
                            "visualizacion": "NO",
                            "tipo_grafica": None,
                            "imagen": None,
                            "link_power_bi": None,
                            "insight": "",
                            "error": f"SQL validation failed after {max_sql_retries} attempts: {', '.join(validation_errors)}",
                        }
                        errors.extend(validation_errors)
                        self.session_logger.end_session(
                            success=False,
                            final_message=json.dumps(error_response, indent=2, ensure_ascii=False),
                            errors=errors,
                        )
                        return error_response

            # Step 6: SQL_EXECUTION
            logger.info(f"{PipelineStep.SQL_EXECUTION.value}: {PipelineStepDescription.SQL_EXECUTION.value}")
            sql_exec_prompt = build_sql_execution_system_prompt()
            start_time = time.time()
            exec_result = await self.sql_exec.execute(state.sql_query)
            execution_time = (time.time() - start_time) * 1000
            state.sql_results = exec_result.get("resultados", [])
            state.total_filas = exec_result.get("total_filas", 0)
            state.sql_resumen = exec_result.get("resumen")
            self.session_logger.log_agent_response(
                agent_name="SQLExecutor",
                raw_response=json.dumps(exec_result, indent=2, ensure_ascii=False),
                parsed_response=exec_result,
                input_text=state.sql_query,
                system_prompt=sql_exec_prompt,
                execution_time_ms=execution_time,
            )

            # Step 7: VERIFICATION
            logger.info(f"{PipelineStep.VERIFICATION.value}: {PipelineStepDescription.VERIFICATION.value}")
            verification_prompt = build_verification_system_prompt() if self.settings.use_llm_verification else None
            start_time = time.time()
            state.verification_passed = await self.verifier.verify(
                state.sql_results, state.sql_query, message
            )
            execution_time = (time.time() - start_time) * 1000
            verification_result = {"passed": state.verification_passed}
            self.session_logger.log_agent_response(
                agent_name="ResultVerifier",
                raw_response=json.dumps(verification_result, indent=2, ensure_ascii=False),
                parsed_response=verification_result,
                input_text=f"SQL: {state.sql_query}\nResults: {len(state.sql_results)} rows",
                system_prompt=verification_prompt,
                execution_time_ms=execution_time,
            )

            # Step 8: VIZ
            if state.viz_required and state.sql_results:
                logger.info(f"{PipelineStep.VIZ.value}: {PipelineStepDescription.VIZ.value}")
                viz_prompt = build_viz_prompt()
                start_time = time.time()
                viz_result = await self.viz.generate(
                    state.sql_results,
                    state.user_id,
                    message,
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
                    input_text=f"Question: {message}\nResults: {len(state.sql_results)} rows",
                    system_prompt=viz_prompt,
                    execution_time_ms=execution_time,
                )

                # Step 9: GRAPH
                if viz_result.get("data_points") and viz_result.get("run_id"):
                    logger.info(f"{PipelineStep.GRAPH.value}: {PipelineStepDescription.GRAPH.value}")
                    try:
                        start_time = time.time()
                        graph_result = await self.graph.generate(
                            run_id=viz_result.get("run_id"),
                            chart_type=viz_result.get("tipo_grafico"),
                            data_points=viz_result.get("data_points", []),
                        )
                        execution_time = (time.time() - start_time) * 1000
                        state.image_url = graph_result.image_url
                        self.session_logger.log_agent_response(
                            agent_name="GraphService",
                            raw_response=json.dumps({"image_url": graph_result.image_url}, indent=2, ensure_ascii=False),
                            parsed_response={"image_url": graph_result.image_url},
                            input_text=f"Run ID: {viz_result.get('run_id')}\nChart Type: {viz_result.get('tipo_grafico')}",
                            execution_time_ms=execution_time,
                        )
                    except Exception as e:
                        logger.warning(f"Graph generation failed: {e}")
                        errors.append(f"Graph generation failed: {str(e)}")

            # Step 10: FORMAT
            logger.info(f"{PipelineStep.FORMAT.value}: {PipelineStepDescription.FORMAT.value}")
            format_prompt = build_format_prompt() if self.settings.use_llm_formatting else None
            start_time = time.time()
            state.final_response = await self.formatter.format(state)
            execution_time = (time.time() - start_time) * 1000
            self.session_logger.log_agent_response(
                agent_name="ResponseFormatter",
                raw_response=json.dumps(state.final_response, indent=2, ensure_ascii=False),
                parsed_response=state.final_response,
                input_text=json.dumps({
                    "intent": state.intent,
                    "pattern_type": state.pattern_type,
                    "arquetipo": state.arquetipo,
                    "sql_results_count": len(state.sql_results),
                }, indent=2, ensure_ascii=False),
                system_prompt=format_prompt,
                execution_time_ms=execution_time,
            )

            self.session_logger.end_session(
                success=True,
                final_message=json.dumps(state.final_response, indent=2, ensure_ascii=False),
                errors=errors,
            )
            return state.final_response

        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            errors.append(f"Pipeline error: {str(e)}")
            self.session_logger.end_session(
                success=False,
                final_message=f"Pipeline error: {str(e)}",
                errors=errors,
            )
            raise

    def _format_non_data_response(
        self, state: PipelineState, triage_result: Dict[str, Any]
    ) -> Dict[str, Any]:
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
        
        # Default format for general questions
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

