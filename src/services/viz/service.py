"""Visualization service."""

import json
import logging
from collections.abc import Callable
from typing import Any

from src.config.prompts import build_viz_mapping_prompt
from src.config.settings import Settings
from src.infrastructure.database import DelfosTools
from src.orchestrator.handlers._llm_helper import run_formatted_handler_agent
from src.services.viz.formatter import build_data_points
from src.services.viz.models import VizColumnMapping

logger = logging.getLogger(__name__)


class VisualizationService:
    """Orchestrates visualization flow."""

    _YEAR_NAMES: frozenset[str] = frozenset({"year", "año", "anio", "yr"})
    _MONTH_NAMES: frozenset[str] = frozenset({"month", "mes", "mn"})
    _SINGLE_SERIES_SUBTYPES: frozenset[str] = frozenset({
        "tendencia_simple", "composicion_simple", "concentracion", "valor_puntual",
        "comparacion_directa", "ranking",
    })

    def __init__(self, settings: Settings, db_tools: DelfosTools | None = None):
        """Initialize visualization service.

        Args:
            settings: Application settings
            db_tools: Optional DelfosTools instance for direct DB access
        """
        self.settings = settings
        self.db_tools = db_tools

    # ------------------------------------------------------------------
    # Public: LLM column mapping only (no data processing)
    # ------------------------------------------------------------------

    async def get_mapping(
        self,
        columns: list[str],
        sample_rows: list[dict[str, Any]],
        question: str,
        chart_type: str | None = None,
        sub_type: str | None = None,
        column_stats: dict[str, Any] | None = None,
    ) -> VizColumnMapping | None:
        """Get column mapping from LLM using column names, sample rows, and column stats.

        Returns VizColumnMapping on success, None on failure.
        """
        try:
            input_dict: dict[str, Any] = {
                "columns": columns,
                "sample_rows": sample_rows,
                "question": question,
                "chart_type": chart_type,
            }
            if column_stats:
                input_dict["column_stats"] = column_stats
            mapping_input = json.dumps(input_dict, ensure_ascii=False)

            system_prompt = build_viz_mapping_prompt(chart_type=chart_type, sub_type=sub_type)

            mapping = await run_formatted_handler_agent(
                self.settings,
                name="VizMappingAgent",
                instructions=system_prompt,
                message=mapping_input,
                response_format=VizColumnMapping,
                model=self.settings.viz_agent_model,
                tools=[],
                max_tokens=self.settings.viz_max_tokens,
                temperature=self.settings.viz_temperature,
            )

            if not isinstance(mapping, VizColumnMapping):
                logger.error("Agent failed to produce column mapping")
                return None

            mapping = self._apply_guards(mapping, columns, column_stats, chart_type, sub_type)
            return mapping

        except Exception as e:
            logger.error("Mapping error: %s", e, exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Public: generate() — backward compat for refresh_graph
    # ------------------------------------------------------------------

    async def generate(
        self,
        sql_results: list[Any],
        user_id: str,
        question: str,
        sql_query: str | None = "",
        tablas: list[str] | None = None,
        resumen: str | None = "",
        chart_type: str | None = None,
        sub_type: str | None = None,
        build_data_points_hook: Callable[..., list[dict[str, Any]]] | None = None,
        precomputed_mapping: VizColumnMapping | None = None,
    ) -> dict[str, Any]:
        """Generate visualization for SQL results (full flow).

        Kept for backward compatibility (refresh_graph).
        New code should use get_mapping() + build_data_points() separately.
        """
        if not sql_results:
            return self._error_result("No SQL results to visualize")

        columns = list(sql_results[0].keys())
        n = len(sql_results)
        if n <= 5:
            sample_rows = sql_results
        else:
            step = max(1, n // 5)
            sample_rows = [sql_results[i * step] for i in range(min(5, n))]

        # Column stats for LLM cardinality awareness
        max_unique_shown = 15
        column_stats: dict[str, Any] = {}
        for col in columns:
            unique_vals = list({row.get(col) for row in sql_results})
            count = len(unique_vals)
            column_stats[col] = {
                "unique_count": count,
                "unique_values": unique_vals[:max_unique_shown] if count <= max_unique_shown else None,
                "sample_values": unique_vals[:5] if count > max_unique_shown else None,
            }

        if precomputed_mapping is not None:
            mapping = self._apply_guards(
                precomputed_mapping, columns, column_stats, chart_type, sub_type,
            )
        else:
            mapping = await self.get_mapping(
                columns, sample_rows, question, chart_type, sub_type,
                column_stats=column_stats,
            )
        if mapping is None:
            return self._error_result("Agent failed to produce column mapping")

        # Build data points
        if build_data_points_hook:
            data_points = build_data_points_hook(sql_results, mapping)
        else:
            data_points = build_data_points(sql_results, mapping)

        data_points = self.limit_categories(data_points, self.settings.viz_max_categories)

        # DB insert
        run_id = None
        powerbi_url = None

        if self.db_tools is not None:
            run_id = self.db_tools.insert_agent_output_batch(
                user_id=user_id,
                question=question,
                results=data_points,
                metric_name=mapping.metric_name,
                visual_hint=chart_type or "barras",
            )
            logger.info("insert_agent_output_batch returned run_id: %s", run_id)

            if run_id:
                powerbi_url = self.db_tools.generate_powerbi_url(
                    run_id=run_id,
                    visual_hint=chart_type or "barras",
                )

        return {
            "tipo_grafico": chart_type,
            "metric_name": mapping.metric_name,
            "x_axis_name": mapping.x_axis_name,
            "y_axis_name": mapping.y_axis_name,
            "series_name": mapping.series_name,
            "category_name": mapping.category_name,
            "data_points": data_points,
            "powerbi_url": powerbi_url,
            "run_id": run_id,
        }

    # ------------------------------------------------------------------
    # Public static: limit_categories (callable from pipeline)
    # ------------------------------------------------------------------

    @staticmethod
    def limit_categories(
        data_points: list[dict[str, Any]],
        max_categories: int,
    ) -> list[dict[str, Any]]:
        """Limit categories to top N-1 + 'Otros', grouped by total y_value."""
        if max_categories < 2:
            return data_points

        category_totals: dict[str, float] = {}
        for dp in data_points:
            cat = dp.get("category") or dp.get("series") or "Sin categoría"
            category_totals[cat] = category_totals.get(cat, 0) + (dp.get("y_value") or 0)

        if len(category_totals) <= max_categories:
            return data_points

        sorted_cats = sorted(category_totals.items(), key=lambda x: x[1], reverse=True)
        top_categories = {cat for cat, _ in sorted_cats[: max_categories - 1]}

        result: list[dict[str, Any]] = []
        otros_by_x: dict[str, dict[str, Any]] = {}

        for dp in data_points:
            cat = dp.get("category") or dp.get("series") or "Sin categoría"
            if cat in top_categories:
                result.append(dp)
            else:
                x = dp.get("x_value", "")
                if x not in otros_by_x:
                    otros_by_x[x] = {
                        "x_value": x,
                        "y_value": 0,
                        "series": "Otros",
                        "category": "Otros",
                    }
                otros_by_x[x]["y_value"] += dp.get("y_value") or 0

        result.extend(otros_by_x.values())
        return result

    # ------------------------------------------------------------------
    # Guards (internal)
    # ------------------------------------------------------------------

    def _apply_guards(
        self,
        mapping: VizColumnMapping,
        columns: list[str],
        column_stats: dict[str, Any] | None,
        chart_type: str | None,
        sub_type: str | None,
    ) -> VizColumnMapping:
        """Apply all post-LLM guards to a mapping."""
        if chart_type != "scatter":
            mapping = self._guard_temporal_columns(mapping, columns)

        if chart_type and "stack" in chart_type.lower():
            mapping = self._guard_stacked_bar_axes(mapping, columns)

        if sub_type and sub_type in self._SINGLE_SERIES_SUBTYPES:
            series_has_multiple = False
            if column_stats and mapping.series_column:
                stats = column_stats.get(mapping.series_column, {})
                series_has_multiple = stats.get("unique_count", 0) > 1

            if not series_has_multiple and (mapping.series_column or mapping.category_column):
                logger.info(
                    "Guard: sub_type=%s is single-series, clearing series=%s category=%s",
                    sub_type, mapping.series_column, mapping.category_column,
                )
                mapping = mapping.model_copy(update={
                    "series_column": None,
                    "category_column": None,
                    "series_name": None,
                    "category_name": None,
                })

        logger.info(
            "Mapping: x=%s, y=%s, month=%s, series=%s, category=%s, x_format=%s",
            mapping.x_column,
            mapping.y_column,
            mapping.month_column,
            mapping.series_column,
            mapping.category_column,
            mapping.x_format,
        )
        return mapping

    def _guard_stacked_bar_axes(
        self,
        mapping: VizColumnMapping,
        columns: list[str],
    ) -> VizColumnMapping:
        """Ensure x_column != series_column for stacked bar charts."""
        if not mapping.series_column or not mapping.x_column:
            return mapping

        if mapping.x_column != mapping.series_column:
            return mapping

        if mapping.category_column and mapping.category_column != mapping.x_column:
            logger.info(
                "Guard: stacked bar x==series (%s). Swapping x→%s",
                mapping.x_column,
                mapping.category_column,
            )
            return mapping.model_copy(update={
                "x_column": mapping.category_column,
                "category_column": mapping.series_column,
            })

        numeric_cols = {mapping.y_column}
        for col in columns:
            if col not in numeric_cols and col != mapping.x_column:
                logger.info(
                    "Guard: stacked bar x==series==category (%s). Using %s for x",
                    mapping.x_column,
                    col,
                )
                return mapping.model_copy(update={
                    "x_column": col,
                    "category_column": mapping.series_column,
                })

        return mapping

    def _guard_temporal_columns(
        self,
        mapping: VizColumnMapping,
        columns: list[str],
    ) -> VizColumnMapping:
        """Ensure year+month separate columns are correctly mapped."""
        if mapping.month_column and mapping.x_format == "YYYY-MM":
            return mapping

        cols_lower = {c.lower(): c for c in columns}
        year_col = next((cols_lower[n] for n in self._YEAR_NAMES if n in cols_lower), None)
        month_col = next((cols_lower[n] for n in self._MONTH_NAMES if n in cols_lower), None)

        if year_col and month_col:
            logger.info("Guard: detected year=%s + month=%s → forcing YYYY-MM", year_col, month_col)
            return mapping.model_copy(update={
                "x_column": year_col,
                "month_column": month_col,
                "x_format": "YYYY-MM",
                "x_axis_name": "Periodo",
            })
        return mapping

    def _error_result(self, error_message: str) -> dict[str, Any]:
        """Build error result dictionary."""
        return {
            "tipo_grafico": None,
            "metric_name": None,
            "x_axis_name": None,
            "y_axis_name": None,
            "series_name": None,
            "category_name": None,
            "data_points": [],
            "powerbi_url": None,
            "run_id": None,
            "error": error_message,
        }
