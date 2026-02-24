"""Standardized response builder for ChatResponse."""

from typing import Any

from src.api.models import ChatResponse


def build_response(
    patron: str,
    insight: str | None = None,
    error: str = "",
    **overrides: Any,
) -> dict[str, Any]:
    """Build a ChatResponse-compatible dict with Pydantic validation."""
    fields: dict[str, Any] = {
        "patron": patron,
        "datos": [],
        "arquetipo": None,
        "visualizacion": "NO",
        "tipo_grafica": None,
        "titulo_grafica": None,
        "data_points": None,
        "metric_name": None,
        "x_axis_name": None,
        "y_axis_name": None,
        "series_name": None,
        "category_name": None,
        "is_tasa": False,
        "link_power_bi": None,
        "insight": insight,
        "sql_query": None,
        "stats_summary": None,
        "error": error,
        "needs_clarification": False,
        "clarification_question": None,
    }
    fields.update(overrides)
    return ChatResponse(**fields).model_dump()
