"""Main pipeline orchestrator."""

import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from src.api.response import build_response
from src.config.archetypes import get_archetype_name
from src.config.subtypes import (
    SubType,
    get_chart_type_for_subtype,
    get_legacy_archetype,
    get_pattern_type,
    get_subtype_from_string,
    get_temporality,
    is_blocked,
)
from src.config.constants import ChartType, PatternType, PipelineStep, QueryType
from src.config.message import get_rejection_message
from src.config.prompts import (
    build_format_prompt,
    build_triage_system_prompt,
    build_viz_mapping_prompt,
)
from src.config.settings import Settings
from src.infrastructure.database import DelfosTools
from src.infrastructure.logging.session_logger import SessionLogger
from src.orchestrator.context import ConversationContext, ConversationStore
from src.orchestrator.handler_router import HandlerRouter
from src.orchestrator.handlers import (
    ClarificationHandler,
    FollowUpHandler,
    GeneralHandler,
    GreetingHandler,
    VizRequestHandler,
)
from src.orchestrator.sql_flow import SQLFlowOrchestrator
from src.orchestrator.state import PipelineState
from src.orchestrator.step_timer import timed_step
from src.services.formatting.formatter import ResponseFormatter
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
        self.greeting_handler = GreetingHandler()
        self.follow_up_handler = FollowUpHandler(settings)
        self.viz_request_handler = VizRequestHandler(settings)
        self.general_handler = GeneralHandler(settings)
        self.clarification_handler = ClarificationHandler(settings)
        self.intent = IntentClassifier(settings)
        self.schema = SchemaService(settings)
        self.sql_gen = SQLGenerator(settings)
        self.sql_validation = SQLValidationService()
        self.sql_exec = SQLExecutor(settings)
        self.verifier = ResultVerifier(settings)
        self.formatter = ResponseFormatter(settings)
        self.session_logger = SessionLogger()
        self.handler_router = HandlerRouter(
            greeting=self.greeting_handler,
            follow_up=self.follow_up_handler,
            viz_request=self.viz_request_handler,
            general=self.general_handler,
            clarification=self.clarification_handler,
        )
        self.sql_flow = SQLFlowOrchestrator(
            settings=settings,
            sql_gen=self.sql_gen,
            sql_validation=self.sql_validation,
            sql_exec=self.sql_exec,
            verifier=self.verifier,
            session_logger=self.session_logger,
        )

        self.db_tools: DelfosTools | None = None
        if settings.use_direct_db:
            from src.infrastructure.database.connection import FabricConnectionFactory

            logger.info("Initializing DelfosTools with dual Fabric connections (WH + DB)")

            if settings.use_service_principal:
                from azure.identity import ClientSecretCredential

                logger.info("Using Service Principal authentication")
                shared_credential = ClientSecretCredential(
                    tenant_id=settings.azure_tenant_id,
                    client_id=settings.azure_client_id,
                    client_secret=settings.azure_client_secret,
                )
            else:
                from azure.identity import DefaultAzureCredential

                logger.info("Using Managed Identity authentication")
                shared_credential = DefaultAzureCredential()

            wh_factory = FabricConnectionFactory(
                settings.wh_server, settings.wh_database, credential=shared_credential
            )
            db_factory = FabricConnectionFactory(
                settings.db_server, settings.db_database, credential=shared_credential
            )

            self.db_tools = DelfosTools(
                wh_factory=wh_factory,
                db_factory=db_factory,
                wh_schema=settings.wh_schema,
                db_schema=settings.db_schema,
                workspace_id=settings.powerbi_workspace_id,
                report_id=settings.powerbi_report_id,
            )

        self.viz = VisualizationService(settings, db_tools=self.db_tools)

    async def close(self) -> None:
        """Close all service connections and cleanup resources."""
        try:
            if self.db_tools is not None:
                self.db_tools.close()
            logger.info("Pipeline resources closed")
        except Exception as e:
            logger.error("Error closing pipeline resources: %s", e, exc_info=True)

    async def __aenter__(self) -> "PipelineOrchestrator":
        """Enter the async context manager."""
        return self

    async def __aexit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        """Exit the async context manager, ensuring resources are cleaned up."""
        try:
            await self.close()
        except Exception as cleanup_error:
            logger.warning("Error during cleanup: %s", cleanup_error)

    async def refresh_graph(
        self,
        sql: str,
        chart_type: str,
        title: str,
        user_id: str,
    ) -> dict[str, Any]:
        """Re-execute a saved query and regenerate its visualization.

        Reuses the same services as the main pipeline (steps 5, 7, 8)
        without triage, intent, schema, or SQL generation.

        Args:
            sql: The stored SQL query to re-execute.
            chart_type: Chart type (pie, bar, line, stackedbar).
            title: Graph title.
            user_id: User identifier.

        Returns:
            Dict with new content URL, row_count, and graph metadata.
        """
        exec_result = await self.sql_exec.execute(sql, db_tools=self.db_tools)
        if not exec_result.get("resultados"):
            return {"error": f"Query returned no results: {exec_result.get('resumen', '')}"}

        viz_result = await self.viz.generate(
            sql_results=exec_result["resultados"],
            user_id=user_id,
            question=title,
            sql_query=sql,
            chart_type=chart_type,
        )
        if not viz_result.get("data_points"):
            return {"error": "Visualization formatting failed"}

        return {
            "data_points": viz_result["data_points"],
            "metric_name": viz_result.get("metric_name"),
            "run_id": viz_result.get("run_id"),
            "row_count": exec_result["total_filas"],
        }

    async def _step_triage(
        self,
        state: PipelineState,
        message: str,
        has_context: bool = False,
        context_summary: str | None = None,
        conversation_history: str | None = None,
        db_tools: DelfosTools | None = None,
    ) -> dict[str, Any]:
        """Execute triage step with context awareness."""
        triage_prompt = build_triage_system_prompt(
            has_context=has_context,
            context_summary=context_summary,
            conversation_history=conversation_history,
        )
        async with timed_step(
            PipelineStep.TRIAGE, self.session_logger, "TriageClassifier",
            input_text=message, system_prompt=triage_prompt,
        ) as ctx:
            triage_result = await self.triage.classify(
                message,
                has_context=has_context,
                context_summary=context_summary,
                conversation_history=conversation_history,
                db_tools=db_tools,
            )

            if not triage_result or "query_type" not in triage_result:
                logger.error(
                    "TriageClassifier returned invalid result: %s. Defaulting to data_question.",
                    triage_result,
                )
                state.query_type = QueryType.DATA_QUESTION
                return {
                    "query_type": QueryType.DATA_QUESTION,
                    "reasoning": "Error parsing triage result, defaulting to data_question",
                }

            state.query_type = triage_result["query_type"]
            ctx.set_result(triage_result)

        return triage_result

    async def _step_intent(
        self,
        state: PipelineState,
        message: str,
        context: ConversationContext | None = None,
    ) -> dict[str, Any]:
        """Execute intent classification step."""
        intent_message = message
        if context and context.last_query and context.last_temporality:
            intent_message = (
                f"## Contexto de conversación\n"
                f'Pregunta anterior: "{context.last_query}"\n'
                f"Clasificación temporal anterior: {context.last_temporality}\n\n"
                f"## Pregunta actual\n{message}"
            )

        async with timed_step(
            PipelineStep.INTENT, self.session_logger, "IntentClassifier",
            input_text=intent_message,
        ) as ctx:
            intent_result = await self.intent.classify(intent_message)

            state.intent = intent_result["intent"]
            state.sub_type = intent_result.get("sub_type", "valor_puntual")
            state.titulo_grafica = intent_result.get("titulo_grafica")
            state.is_tasa = intent_result.get("is_tasa", False)

            # Parse and validate sub_type
            sub_type_enum = get_subtype_from_string(state.sub_type)
            if sub_type_enum is None:
                logger.warning(
                    "Invalid sub_type '%s', defaulting to valor_puntual", state.sub_type
                )
                sub_type_enum = SubType.VALOR_PUNTUAL
                state.sub_type = sub_type_enum.value

            # Auto-populate legacy fields for backward compatibility
            state.arquetipo = get_archetype_name(get_legacy_archetype(sub_type_enum))
            state.viz_required = state.intent == "requiere_visualizacion"
            state.temporality = get_temporality(sub_type_enum)
            state.pattern_type = get_pattern_type(sub_type_enum)

            ctx.set_result(intent_result)

        # Gate: blocked sub_types get a "not supported" response
        if is_blocked(sub_type_enum):
            response = self._format_non_comparacion_response(state, intent_result)
            self.session_logger.end_session(
                success=True,
                final_message=json.dumps(response, indent=2, ensure_ascii=False),
                errors=[],
            )
            return response

        return intent_result

    async def _step_schema(
        self,
        state: PipelineState,
        message: str,
        db_tools: DelfosTools | None = None,
    ) -> dict[str, Any]:
        """Execute schema selection step."""
        async with timed_step(
            PipelineStep.SCHEMA, self.session_logger, "SchemaService",
            input_text=message,
        ) as ctx:
            schema_result = await self.schema.get_schema_context(message, db_tools=db_tools)
            state.selected_tables = schema_result.get("tables", [])
            state.schema_context = schema_result
            ctx.set_result(schema_result)
        return schema_result

    async def _step_visualization(
        self,
        state: PipelineState,
        message: str,
        db_tools: DelfosTools | None = None,
    ) -> dict[str, Any] | None:
        """Execute visualization step."""
        if not (state.viz_required and state.sql_results):
            return None

        sub_type_enum = get_subtype_from_string(state.sub_type or "valor_puntual")
        if sub_type_enum:
            state.tipo_grafico = get_chart_type_for_subtype(sub_type_enum)
        else:
            state.tipo_grafico = None
        logger.info(
            "Determined chart type: %s for sub_type: %s", state.tipo_grafico, state.sub_type
        )

        if state.tipo_grafico == ChartType.STACKED_BAR:
            state.tipo_grafico = self._guard_stacked_bar(state.sql_results)

        viz_prompt = build_viz_mapping_prompt(
            chart_type=state.tipo_grafico, sub_type=state.sub_type,
        )
        viz_input = {
            "user_id": state.user_id,
            "columns": list(state.sql_results[0].keys()) if state.sql_results else [],
            "sample_rows": (state.sql_results or [])[:3],
            "total_filas": len(state.sql_results or []),
            "original_question": message,
            "tipo_grafico": state.tipo_grafico,
            "sub_type": state.sub_type,
        }

        async with timed_step(
            PipelineStep.VIZ, self.session_logger, "VisualizationService",
            input_text=json.dumps(viz_input, indent=2, ensure_ascii=False),
            system_prompt=viz_prompt,
        ) as ctx:
            viz_result = await self.viz.generate(
                state.sql_results,
                state.user_id,
                message,
                sql_query=state.sql_query,
                tablas=state.selected_tables,
                resumen=state.sql_resumen,
                chart_type=state.tipo_grafico,
                sub_type=state.sub_type,
            )
            state.powerbi_url = viz_result.get("powerbi_url")
            state.data_points = viz_result.get("data_points")
            state.metric_name = viz_result.get("metric_name")
            state.run_id = viz_result.get("run_id")
            state.x_axis_name = viz_result.get("x_axis_name")
            state.y_axis_name = viz_result.get("y_axis_name")
            state.series_name = viz_result.get("series_name")
            state.category_name = viz_result.get("category_name")
            ctx.set_result(viz_result)
        return viz_result

    async def _step_format(self, state: PipelineState) -> dict[str, Any]:
        """Execute response formatting step."""
        format_prompt = build_format_prompt() if self.settings.use_llm_formatting else None
        format_input = json.dumps(
            {
                "intent": state.intent,
                "pattern_type": state.pattern_type,
                "arquetipo": state.arquetipo,
                "sql_results_count": len(state.sql_results or []),
            },
            indent=2,
            ensure_ascii=False,
        )
        async with timed_step(
            PipelineStep.FORMAT, self.session_logger, "ResponseFormatter",
            input_text=format_input, system_prompt=format_prompt,
        ) as ctx:
            state.final_response = await self.formatter.format(state)
            ctx.set_result(state.final_response)
        return state.final_response

    @staticmethod
    def _build_sql_message(message: str, context: ConversationContext) -> str:
        """Enriquecer mensaje con contexto de conversacion para preguntas de seguimiento."""
        if not context.last_query:
            return message

        parts = [
            "## Contexto de conversación",
            f"Pregunta anterior: \"{context.last_query}\"",
        ]

        if context.last_sql:
            parts.append(
                "SQL anterior (referencia de tablas, columnas y entidades usadas):\n"
                f"```sql\n{context.last_sql}\n```\n"
                "**IMPORTANTE**: El SQL anterior es SOLO referencia para identificar"
                " tablas, entidades y métricas relevantes."
                " La estructura temporal (agrupación por año/mes vs. agregado estático)"
                " se define por las instrucciones del sistema, NO por el SQL anterior.\n"
                "**ADVERTENCIA**: Los nombres de entidad (NOMBRE_ENTIDAD) del SQL anterior"
                " pueden ser INCORRECTOS. SIEMPRE verificar con"
                " get_distinct_values antes de usarlos en WHERE."
            )

        if context.last_columns:
            parts.append(f"Columnas resultado: {', '.join(context.last_columns)}")

        if context.last_tables:
            parts.append(f"Tablas usadas: {', '.join(context.last_tables)}")

        parts.append(f"\n## Pregunta actual\n{message}")
        return "\n".join(parts)

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

        try:
            context = ConversationStore.get(user_id)
            has_context = bool(context.last_results)
            context_summary = context.get_summary() if has_context else None
            conversation_history = context.get_history_summary(self.settings.max_history_turns)

            if context_summary:
                logger.info(
                    "Context available: %s rows from previous query",
                    len(context.last_results or []),
                )

            # Record user turn in history BEFORE processing
            ConversationStore.add_turn(
                user_id, "user", message,
                max_history_turns=self.settings.max_history_turns,
            )

            await self._step_triage(
                state, message, has_context, context_summary,
                conversation_history=conversation_history,
                db_tools=self.db_tools,
            )

            handler_response = await self.handler_router.route(state, message, user_id, context)
            if handler_response is not None:
                # Record assistant turn for handler responses
                response_text = handler_response.get("insight") or handler_response.get("clarification_question") or ""
                ConversationStore.add_turn(
                    user_id, "assistant", response_text,
                    query_type=state.query_type,
                    had_viz=handler_response.get("visualizacion") == "YES",
                    max_history_turns=self.settings.max_history_turns,
                )
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(handler_response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                return handler_response

            intent_result = await self._step_intent(state, message, context=context)
            if state.pattern_type != PatternType.COMPARACION:
                # Record assistant turn for non-comparacion
                ConversationStore.add_turn(
                    user_id, "assistant", intent_result.get("reasoning", ""),
                    query_type=state.query_type,
                    max_history_turns=self.settings.max_history_turns,
                )
                return intent_result

            await self._step_schema(state, message, db_tools=self.db_tools)

            sql_message = self._build_sql_message(message, context)

            sql_error = await self.sql_flow.execute(
                state,
                sql_message,
                db_tools=self.db_tools,
            )
            if sql_error:
                errors.append(sql_error.get("error", ""))
                self.session_logger.end_session(
                    success=False,
                    final_message=json.dumps(sql_error, indent=2, ensure_ascii=False),
                    errors=errors,
                )
                return sql_error

            viz_result = await self._step_visualization(
                state, message, db_tools=self.db_tools
            )

            final_response = await self._step_format(state)

            ConversationStore.update(
                user_id=user_id,
                query=message,
                sql=state.sql_query,
                results=state.sql_results,
                response=final_response,
                chart_type=state.tipo_grafico,
                run_id=state.run_id,
                data_points=viz_result.get("data_points") if viz_result else None,
                tables=state.resolved_tables,
                schema_context=state.schema_context,
                title=state.titulo_grafica,
                temporality=state.temporality,
            )

            # Record assistant turn for data queries
            ConversationStore.add_turn(
                user_id, "assistant", final_response.get("insight", ""),
                query_type=state.query_type,
                had_viz=state.viz_required,
                tables_used=state.resolved_tables,
                max_history_turns=self.settings.max_history_turns,
            )

            self.session_logger.end_session(
                success=True,
                final_message=json.dumps(final_response, indent=2, ensure_ascii=False),
                errors=errors,
            )
            return final_response

        except Exception as e:
            logger.error("Pipeline error: %s", e, exc_info=True)
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
        errors: list[str] = []

        self.session_logger.start_session(user_id=user_id, user_message=message)

        try:
            context = ConversationStore.get(user_id)
            has_context = bool(context.last_results)
            context_summary = context.get_summary() if has_context else None
            conversation_history = context.get_history_summary(self.settings.max_history_turns)

            if context_summary:
                logger.info(
                    "Context available: %s rows from previous query",
                    len(context.last_results or []),
                )

            # Record user turn in history BEFORE processing
            ConversationStore.add_turn(
                user_id, "user", message,
                max_history_turns=self.settings.max_history_turns,
            )

            triage_result = await self._step_triage(
                state, message, has_context, context_summary,
                conversation_history=conversation_history,
                db_tools=self.db_tools,
            )
            yield {
                "step": "triage",
                "result": triage_result,
                "state": {"query_type": state.query_type},
            }

            handler_response = await self.handler_router.route(state, message, user_id, context)
            if handler_response is not None:
                # Record assistant turn for handler responses
                response_text = handler_response.get("insight") or handler_response.get("clarification_question") or ""
                ConversationStore.add_turn(
                    user_id, "assistant", response_text,
                    query_type=state.query_type,
                    had_viz=handler_response.get("visualizacion") == "YES",
                    max_history_turns=self.settings.max_history_turns,
                )
                self.session_logger.end_session(
                    success=True,
                    final_message=json.dumps(handler_response, indent=2, ensure_ascii=False),
                    errors=[],
                )
                yield {"step": "complete", "response": handler_response}
                return

            intent_result = await self._step_intent(state, message, context=context)
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
            if state.pattern_type != PatternType.COMPARACION:
                # Record assistant turn for non-comparacion
                ConversationStore.add_turn(
                    user_id, "assistant", intent_result.get("reasoning", ""),
                    query_type=state.query_type,
                    max_history_turns=self.settings.max_history_turns,
                )
                yield {"step": "complete", "response": intent_result}
                return

            schema_result = await self._step_schema(state, message, db_tools=self.db_tools)
            yield {
                "step": "schema",
                "result": schema_result,
                "state": {"selected_tables": state.selected_tables},
            }

            sql_message = self._build_sql_message(message, context)

            async for sql_event in self.sql_flow.execute_streaming(
                state,
                sql_message,
                db_tools=self.db_tools,
            ):
                yield sql_event

                if sql_event.get("step") == "sql_generation" and sql_event.get("result", {}).get(
                    "error"
                ):
                    errors.append(sql_event["result"].get("error", ""))
                    self.session_logger.end_session(
                        success=False,
                        final_message=json.dumps(sql_event["result"], indent=2, ensure_ascii=False),
                        errors=errors,
                    )
                    yield {"step": "complete", "response": sql_event["result"]}
                    return

            viz_result = await self._step_visualization(
                state, message, db_tools=self.db_tools
            )
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

            # Step: FORMAT
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
                tables=state.resolved_tables,
                schema_context=state.schema_context,
                title=state.titulo_grafica,
                temporality=state.temporality,
            )

            # Record assistant turn for data queries
            ConversationStore.add_turn(
                user_id, "assistant", final_response.get("insight", ""),
                query_type=state.query_type,
                had_viz=state.viz_required,
                tables_used=state.resolved_tables,
                max_history_turns=self.settings.max_history_turns,
            )

            self.session_logger.end_session(
                success=True,
                final_message=json.dumps(final_response, indent=2, ensure_ascii=False),
                errors=errors,
            )
            yield {"step": "complete", "response": final_response}

        except Exception as e:
            logger.error("Pipeline error: %s", e, exc_info=True)
            errors.append(f"Pipeline error: {str(e)}")
            self.session_logger.end_session(
                success=False,
                final_message=f"Pipeline error: {str(e)}",
                errors=errors,
            )
            yield {"step": "error", "error": str(e)}

    _TEMPORAL_COLUMNS = {"year", "month", "fecha", "periodo", "date"}

    def _guard_stacked_bar(self, rows: list[dict[str, Any]] | None) -> ChartType:
        """Validate that data has a categorical column for stacking.

        Stacked bar charts need at least one non-temporal string column.
        Falls back to LINE when the data is purely numeric.
        """
        if not rows:
            return ChartType.STACKED_BAR

        first_row = rows[0]
        has_categorical = any(
            isinstance(value, str)
            for col, value in first_row.items()
            if col.lower() not in self._TEMPORAL_COLUMNS
        )
        if not has_categorical:
            logger.info("No categorical column for stacking; falling back to LINE")
            return ChartType.LINE
        return ChartType.STACKED_BAR

    def _format_non_data_response(
        self, state: PipelineState, triage_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Format response for non-data questions (general, out_of_scope)."""
        query_type_str = state.query_type or QueryType.GENERAL
        try:
            query_type = QueryType(query_type_str)
        except ValueError:
            query_type = QueryType.GENERAL

        message = get_rejection_message(query_type)
        reasoning = triage_result.get("reasoning", message)

        return build_response(patron=QueryType.GENERAL, insight=reasoning)

    def _format_non_comparacion_response(
        self, state: PipelineState, intent_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Format response for non-comparacion questions."""
        reasoning = intent_result.get(
            "reasoning",
            "Este tipo de pregunta aun no esta soportada. Por favor, ingrese una pregunta de comparacion.",
        )
        return build_response(
            patron=state.pattern_type or "comparacion",
            datos=[{"NA": {}}],
            arquetipo=state.arquetipo,
            visualizacion="NA",
            tipo_grafica="NA",
            titulo_grafica=state.titulo_grafica,
            link_power_bi="NA",
            insight="NA",
            error=reasoning,
        )
