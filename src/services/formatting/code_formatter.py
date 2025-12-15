"""Code-based response formatter (no LLM)."""

import logging
from typing import Any

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
            response = {
                "patron": state.pattern_type or "general",
                "datos": state.sql_results or [],
                "arquetipo": state.arquetipo or "A",
                "visualizacion": "YES" if state.viz_required and state.powerbi_url else "NO",
                "tipo_grafica": state.tipo_grafico,
                "imagen": state.image_url,
                "html_url": state.html_url,
                "link_power_bi": state.powerbi_url,
                "insight": state.sql_insights or state.sql_resumen or "No insight available",
                "error": "",
            }

            return response

        except Exception as e:
            logger.error(f"Code formatting error: {e}", exc_info=True)
            # Fallback response
            return {
                "patron": "general",
                "datos": state.sql_results or [],
                "arquetipo": state.pattern_type or "A",
                "visualizacion": "NO",
                "tipo_grafica": None,
                "imagen": None,
                "html_url": None,
                "link_power_bi": None,
                "insight": state.sql_insights,
                "error": str(e),
            }
