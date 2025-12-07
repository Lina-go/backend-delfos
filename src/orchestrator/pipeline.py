"""Main pipeline orchestrator."""

import logging
from typing import Dict, Any

from src.config.settings import Settings
from src.config.constants import PipelineStep, PipelineStepDescription
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

        try:
            # Step 1: TRIAGE
            logger.info(f"{PipelineStep.TRIAGE.value}: {PipelineStepDescription.TRIAGE.value}")
            triage_result = await self.triage.classify(message)
            state.query_type = triage_result["query_type"]

            if state.query_type != "data_question":
                # Return early for non-data questions
                return self._format_non_data_response(state, triage_result)

            # Step 2: INTENT
            logger.info(f"{PipelineStep.INTENT.value}: {PipelineStepDescription.INTENT.value}")
            intent_result = await self.intent.classify(message)
            state.intent = intent_result["intent"]
            state.pattern_type = intent_result.get("pattern_type")
            state.arquetipo = intent_result.get("arquetipo")
            state.viz_required = state.intent == "requiere_viz"

            # Step 3: SCHEMA
            logger.info(f"{PipelineStep.SCHEMA.value}: {PipelineStepDescription.SCHEMA.value}")
            schema_result = await self.schema.get_schema_context(message)
            state.selected_tables = schema_result.get("tables", [])
            state.schema_context = schema_result

            # Step 4: SQL_GENERATION
            logger.info(f"{PipelineStep.SQL_GENERATION.value}: {PipelineStepDescription.SQL_GENERATION.value}")
            sql_result = await self.sql_gen.generate(
                message,
                state.schema_context,
                intent=state.intent,
                pattern_type=state.pattern_type,
                arquetipo=state.arquetipo
            )
            state.sql_query = sql_result.get("sql")

            # Step 5: SQL_VALIDATION
            logger.info(f"{PipelineStep.SQL_VALIDATION.value}: {PipelineStepDescription.SQL_VALIDATION.value}")
            validation_result = self.sql_validation.validate(state.sql_query)
            if not validation_result["is_valid"]:
                logger.error(f"SQL validation failed: {validation_result['errors']}")
                return {
                    "patron": "error",
                    "datos": [],
                    "arquetipo": state.arquetipo,
                    "visualizacion": "NO",
                    "tipo_grafica": None,
                    "imagen": None,
                    "link_power_bi": None,
                    "insight": f"SQL validation failed: {', '.join(validation_result['errors'])}",
                }

            # Step 6: SQL_EXECUTION
            logger.info(f"{PipelineStep.SQL_EXECUTION.value}: {PipelineStepDescription.SQL_EXECUTION.value}")
            exec_result = await self.sql_exec.execute(state.sql_query)
            state.sql_results = exec_result.get("resultados", [])
            state.total_filas = exec_result.get("total_filas", 0)
            state.sql_resumen = exec_result.get("resumen")

            # Step 7: VERIFICATION
            logger.info(f"{PipelineStep.VERIFICATION.value}: {PipelineStepDescription.VERIFICATION.value}")
            state.verification_passed = await self.verifier.verify(
                state.sql_results, state.sql_query, message
            )

            # Step 8: VIZ
            if state.viz_required and state.sql_results:
                logger.info(f"{PipelineStep.VIZ.value}: {PipelineStepDescription.VIZ.value}")
                viz_result = await self.viz.generate(
                    state.sql_results,
                    state.user_id,
                    message,
                )
                state.tipo_grafico = viz_result.get("tipo_grafico")
                state.powerbi_url = viz_result.get("powerbi_url")
                state.image_url = viz_result.get("image_url")
                state.run_id = viz_result.get("run_id")

                # Step 9: GRAPH
                if viz_result.get("data_points") and viz_result.get("run_id"):
                    logger.info(f"{PipelineStep.GRAPH.value}: {PipelineStepDescription.GRAPH.value}")
                    try:
                        graph_result = await self.graph.generate(
                            run_id=viz_result.get("run_id"),
                            chart_type=viz_result.get("tipo_grafico"),
                            data_points=viz_result.get("data_points", []),
                        )
                        state.image_url = graph_result.image_url
                    except Exception as e:
                        logger.warning(f"Graph generation failed: {e}")

            # Step 10: FORMAT
            logger.info(f"{PipelineStep.FORMAT.value}: {PipelineStepDescription.FORMAT.value}")
            state.final_response = await self.formatter.format(state)

            return state.final_response

        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            raise

    def _format_non_data_response(
        self, state: PipelineState, triage_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Format response for non-data questions."""
        return {
            "patron": "general",
            "datos": [],
            "arquetipo": None,
            "visualizacion": "NO",
            "tipo_grafica": None,
            "imagen": None,
            "link_power_bi": None,
            "insight": triage_result.get("reasoning", "This is not a data question."),
        }

