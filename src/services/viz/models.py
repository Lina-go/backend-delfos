"""Visualization service models."""

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel


class VizResult(BaseModel):
    """Result from visualization generation."""

    tipo_grafico: str | None = None
    metric_name: str | None = None
    data_points: list[dict[str, Any]] | None = None
    powerbi_url: str | None = None
    run_id: str | None = None
    image_url: str | None = None


@dataclass
class ChartConfig:
    """Configuration for chart generation."""

    chart_type: str
    data_points: list[dict[str, Any]]
    title: str | None = None
    x_label: str | None = None
    y_label: str | None = None
