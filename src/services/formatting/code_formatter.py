"""Code-based response formatter (no LLM)."""

import logging
from typing import Any

from src.api.response import build_response
from src.config.constants import Archetype, QueryType
from src.orchestrator.state import PipelineState

logger = logging.getLogger(__name__)


class CodeFormatter:
    """Formats final response using code (no LLM)."""

    @staticmethod
    def format(state: PipelineState) -> dict[str, Any]:
        """
        Format final response from pipeline state using code.

        Args:
            state: Pipeline state object

        Returns:
            Formatted response dictionary
        """
        try:
            return build_response(
                patron=state.pattern_type or QueryType.GENERAL,
                datos=state.sql_results or [],
                arquetipo=state.arquetipo or Archetype.ARCHETYPE_A,
                visualizacion="YES" if state.viz_required and state.powerbi_url else "NO",
                tipo_grafica=state.tipo_grafico,
                titulo_grafica=state.titulo_grafica,
                data_points=state.data_points,
                metric_name=state.metric_name,
                x_axis_name=state.x_axis_name,
                y_axis_name=state.y_axis_name,
                series_name=state.series_name,
                category_name=state.category_name,
                is_tasa=state.is_tasa,
                link_power_bi=state.powerbi_url,
                insight=state.sql_insights or state.sql_resumen or "No insight available",
                sql_query=state.sql_query,
            )

        except Exception as e:
            logger.error("Code formatting error: %s", e, exc_info=True)
            return build_response(
                patron=QueryType.GENERAL,
                datos=state.sql_results or [],
                arquetipo=state.pattern_type or Archetype.ARCHETYPE_A,
                titulo_grafica=state.titulo_grafica,
                insight=state.sql_insights,
                error=str(e),
            )
