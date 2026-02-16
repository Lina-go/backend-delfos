"""Pure-Python data-point formatter — no LLM needed."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.services.viz.models import VizColumnMapping


def build_data_points(
    sql_results: list[dict[str, Any]],
    mapping: VizColumnMapping,
) -> list[dict[str, Any]]:
    """Build visualization data_points from SQL rows using the LLM column mapping.

    This replaces the LLM's mechanical row-by-row formatting.
    Runs in <100ms even for thousands of rows.
    """
    points: list[dict[str, Any]] = []
    for row in sql_results:
        # When year and month are separate columns, combine them
        if mapping.month_column and mapping.x_format == "YYYY-MM":
            year = int(row.get(mapping.x_column, 0))
            month = int(row.get(mapping.month_column, 0))
            x_val = f"{year}-{month:02d}"
        else:
            x_raw = row.get(mapping.x_column, "")
            x_val = _format_value(x_raw, mapping.x_format)

        y_raw = row.get(mapping.y_column, 0)
        y_val = _safe_float(y_raw)

        point: dict[str, Any] = {"x_value": x_val, "y_value": y_val}

        # Resolve series/category labels
        default_label = mapping.metric_name or "Valor"

        if mapping.series_column:
            point["series"] = str(row.get(mapping.series_column, ""))
        else:
            point["series"] = default_label

        if mapping.category_column:
            point["category"] = str(row.get(mapping.category_column, ""))
        else:
            point["category"] = point["series"]

        points.append(point)
    return points


def _format_value(value: Any, fmt: str | None) -> str:
    """Format a value according to the specified format.

    Supports:
    - ``YYYY-MM``: numeric or string date encoding (e.g. 202401 / "202401" → "2024-01")
    - ``None``: plain string conversion
    """
    if fmt == "YYYY-MM":
        try:
            v = int(value)
        except (ValueError, TypeError):
            return str(value) if value is not None else ""
        year, month = divmod(v, 100)
        return f"{year}-{month:02d}"
    return str(value) if value is not None else ""


def _safe_float(value: Any) -> float:
    """Convert *value* to float, returning 0.0 on failure."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0
