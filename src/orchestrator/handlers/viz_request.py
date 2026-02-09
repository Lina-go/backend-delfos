"""Viz request handler - generates charts without re-running SQL."""

import logging
from typing import Any

from src.config.settings import Settings
from src.orchestrator.context import ConversationContext
from src.services.graph.service import GraphService

logger = logging.getLogger(__name__)


class VizRequestHandler:
    """Handles visualization requests using existing data."""

    CHART_KEYWORDS = {
        "pie": ["pie", "pastel", "torta", "circular"],
        "line": ["línea", "linea", "line", "tiempo", "tendencia"],
        "bar": ["barra", "barras", "bar"],
        "stackedbar": ["stacked", "apilad", "acumulad"],
    }

    def __init__(self, settings: Settings):
        self.settings = settings
        self.graph_service = GraphService(settings)

    async def handle(
        self, message: str, user_id: str, context: ConversationContext
    ) -> dict[str, Any]:
        """Handle visualization request using existing data."""
        if not context.last_results:
            return self._no_data_response()

        chart_type = self._detect_chart_type(message) or context.last_chart_type or "bar"

        # Get run_id and data_points from context
        run_id = context.last_run_id
        data_points = context.last_data_points

        if not run_id or not data_points:
            return self._error_response(
                "No hay datos de visualización previos para regenerar el gráfico."
            )

        try:
            graph_result = await self.graph_service.generate(
                run_id=run_id,
                chart_type=chart_type,
                data_points=data_points,
                title=context.last_query or "",
            )

            return {
                "patron": "viz_request",
                "datos": context.last_results,
                "arquetipo": "NA",
                "visualizacion": "SI",
                "tipo_grafica": chart_type,
                "titulo_grafica": context.last_title,
                "imagen": graph_result.image_url,
                "html_url": graph_result.html_url,
                "png_url": graph_result.png_url,
                "link_power_bi": None,
                "insight": f"Aquí están los datos en gráfico de {chart_type}.",
            }

        except Exception as e:
            logger.error(f"Error generating visualization: {e}")
            return self._error_response(str(e))

    def _detect_chart_type(self, message: str) -> str | None:
        """Detect chart type from message keywords."""
        msg_lower = message.lower()

        for chart_type, keywords in self.CHART_KEYWORDS.items():
            if any(kw in msg_lower for kw in keywords):
                return chart_type

        return None

    def _no_data_response(self) -> dict[str, Any]:
        """Return response when there's no data to visualize."""
        return {
            "patron": "viz_request",
            "datos": [],
            "arquetipo": "NA",
            "visualizacion": "NO",
            "tipo_grafica": None,
            "titulo_grafica": None,
            "imagen": None,
            "link_power_bi": None,
            "insight": "No hay datos previos para graficar. Primero haz una consulta de datos.",
        }

    def _error_response(self, error: str) -> dict[str, Any]:
        """Return response when visualization fails."""
        return {
            "patron": "viz_request",
            "datos": [],
            "arquetipo": "NA",
            "visualizacion": "NO",
            "tipo_grafica": None,
            "titulo_grafica": None,
            "imagen": None,
            "link_power_bi": None,
            "insight": f"No pude generar el gráfico: {error}",
        }