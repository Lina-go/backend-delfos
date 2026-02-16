"""Visualization service."""

import json
import logging
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

    def __init__(self, settings: Settings, db_tools: DelfosTools | None = None):
        """Initialize visualization service.

        Args:
            settings: Application settings
            db_tools: Optional DelfosTools instance for direct DB access
        """
        self.settings = settings
        self.db_tools = db_tools

    async def generate(
        self,
        sql_results: list[Any],
        user_id: str,
        question: str,
        sql_query: str | None = "",
        tablas: list[str] | None = None,
        resumen: str | None = "",
        chart_type: str | None = None,
    ) -> dict[str, Any]:
        """Generate visualization for SQL results.

        Hybrid approach:
          Phase A — LLM receives column names + 3 sample rows → returns column mapping (~1-3s)
          Phase B — Pure Python formats ALL rows using the mapping (<100ms)
        """
        try:
            if not sql_results:
                return self._error_result("No SQL results to visualize")

            # --- Phase A: Lightweight LLM call (column mapping only) ---
            columns = list(sql_results[0].keys())
            sample_rows = sql_results[:3]

            mapping_input = json.dumps(
                {
                    "columns": columns,
                    "sample_rows": sample_rows,
                    "question": question,
                    "chart_type": chart_type,
                },
                ensure_ascii=False,
            )

            system_prompt = build_viz_mapping_prompt(chart_type=chart_type)

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
                return self._error_result("Agent failed to produce column mapping")

            # --- Guard: detect year+month columns ---
            mapping = self._guard_temporal_columns(mapping, columns)

            # --- Guard: stacked bar x != series ---
            if chart_type and "stack" in chart_type.lower():
                mapping = self._guard_stacked_bar_axes(mapping, columns)

            logger.info(
                "LLM mapping: x=%s, y=%s, month=%s, series=%s, category=%s, x_format=%s",
                mapping.x_column,
                mapping.y_column,
                mapping.month_column,
                mapping.series_column,
                mapping.category_column,
                mapping.x_format,
            )

            # --- Phase B: Pure Python formatting (<100ms) ---
            data_points = build_data_points(sql_results, mapping)
            logger.info("Python formatted %s data points", len(data_points))

            data_points = self._limit_categories(
                data_points,
                max_categories=self.settings.viz_max_categories,
            )
            logger.info("After category limiting: %s data points", len(data_points))

            # --- DB insert ---
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
                    logger.info("generate_powerbi_url returned URL")

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

        except Exception as e:
            logger.error("Visualization error: %s", e, exc_info=True)
            return self._error_result(str(e))

    def _limit_categories(
        self,
        data_points: list[dict[str, Any]],
        max_categories: int,
    ) -> list[dict[str, Any]]:
        """Limit categories to top N-1 + 'Otros', grouped by total y_value."""
        if max_categories < 2:
            return data_points

        # 1. Sum y_value per category across all x_values
        category_totals: dict[str, float] = {}
        for dp in data_points:
            cat = dp.get("category") or dp.get("series") or "Sin categoría"
            category_totals[cat] = category_totals.get(cat, 0) + (dp.get("y_value") or 0)

        if len(category_totals) <= max_categories:
            return data_points

        # 2. Find top (max - 1) categories by total y_value
        sorted_cats = sorted(category_totals.items(), key=lambda x: x[1], reverse=True)
        top_categories = {cat for cat, _ in sorted_cats[: max_categories - 1]}

        # 3. Keep top categories, aggregate rest into "Otros" per x_value
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

    def _guard_stacked_bar_axes(
        self,
        mapping: VizColumnMapping,
        columns: list[str],
    ) -> VizColumnMapping:
        """Ensure x_column != series_column for stacked bar charts.

        When the LLM puts the same column as both x and series, the chart is
        meaningless. Swap x with category if they're different, or find
        another categorical column.
        """
        if not mapping.series_column or not mapping.x_column:
            return mapping

        if mapping.x_column != mapping.series_column:
            return mapping  # Already correct

        # x == series — need to find the OTHER categorical column for x
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

        # category == series == x: look for any other non-numeric column
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
        """Ensure year+month separate columns are correctly mapped.

        The LLM sometimes ignores the temporal format rule. This guard
        detects year+month column pairs and forces YYYY-MM formatting.
        """
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
