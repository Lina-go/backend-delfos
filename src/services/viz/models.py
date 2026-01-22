"""Visualization service models."""

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field

class VizFormattingResult(BaseModel):
    """
    Result from the LLM agent - ONLY formatting/intelligence.
    
    The agent does NOT call MCP tools. That's the service's job.
    """
    
    metric_name: str = Field(description="Descriptive name of the metric being measured")
    data_points: list[dict[str, Any]] = Field(description="Formatted array of data objects with x_value, y_value, series, category")

class VizResult(BaseModel):
    """Result from visualization generation."""

    tipo_grafico: str | None = None
    metric_name: str | None = None
    data_points: list[dict[str, Any]] | None = None
    powerbi_url: str | None = None
    run_id: str | None = None
    image_url: str | None = None
    error: str | None = None


@dataclass
class ChartConfig:
    """Configuration for chart generation."""

    chart_type: str
    data_points: list[dict[str, Any]]
    title: str | None = None
    x_label: str | None = None
    y_label: str | None = None
