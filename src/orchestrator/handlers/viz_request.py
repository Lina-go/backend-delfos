"""Visualization request handler."""

import logging
from typing import Any

from src.api.response import build_response
from src.config.constants import ChartType, QueryType
from src.config.settings import Settings
from src.orchestrator.context import ConversationContext

logger = logging.getLogger(__name__)


class VizRequestHandler:
    """Handles chart re-generation using existing query data."""

    CHART_KEYWORDS: dict[ChartType, list[str]] = {
        ChartType.PIE: ["pie", "pastel", "torta", "circular"],
        ChartType.LINE: ["línea", "linea", "line", "tiempo", "tendencia"],
        ChartType.BAR: ["barra", "barras", "bar"],
        ChartType.STACKED_BAR: ["stacked", "apilad", "acumulad"],
    }

    def __init__(self, settings: Settings):
        self.settings = settings

    async def handle(
        self, message: str, user_id: str, context: ConversationContext
    ) -> dict[str, Any]:
        """Regenerate a chart from previous query data."""
        if not context.last_results:
            return self._no_data_response()

        chart_type = self._detect_chart_type(message) or context.last_chart_type or ChartType.BAR
        data_points = context.last_data_points

        if not data_points:
            return self._error_response(
                "No hay datos de visualización previos para regenerar el gráfico."
            )

        # Extract axis labels from previous response
        prev = context.last_response or {}

        return build_response(
            patron=QueryType.VIZ_REQUEST,
            datos=context.last_results,
            arquetipo=prev.get("arquetipo", "NA"),
            visualizacion="YES",
            tipo_grafica=chart_type,
            titulo_grafica=context.last_title,
            data_points=data_points,
            metric_name=prev.get("metric_name"),
            x_axis_name=prev.get("x_axis_name"),
            y_axis_name=prev.get("y_axis_name"),
            series_name=prev.get("series_name"),
            category_name=prev.get("category_name"),
            is_tasa=prev.get("is_tasa", False),
            sql_query=prev.get("sql_query"),
            link_power_bi=prev.get("link_power_bi"),
            insight=f"Aquí están los datos en gráfico de {chart_type}.",
        )

    def _detect_chart_type(self, message: str) -> ChartType | None:
        """Detect chart type from keyword matching."""
        msg_lower = message.lower()

        for chart_type, keywords in self.CHART_KEYWORDS.items():
            if any(kw in msg_lower for kw in keywords):
                return chart_type

        return None

    def _no_data_response(self) -> dict[str, Any]:
        """Build response when no data is available."""
        return build_response(
            patron=QueryType.VIZ_REQUEST,
            arquetipo="NA",
            insight="No hay datos previos para graficar. Primero haz una consulta de datos.",
        )

    def _error_response(self, error: str) -> dict[str, Any]:
        """Build error response for failed visualization."""
        return build_response(
            patron=QueryType.VIZ_REQUEST,
            arquetipo="NA",
            insight=f"No pude generar el gráfico: {error}",
        )
