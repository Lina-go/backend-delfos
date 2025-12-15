"""Graph service models."""

from dataclasses import dataclass
from typing import Any


@dataclass
class GraphResult:
    """Result from graph generation."""

    image_url: str | None
    chart_type: str
    run_id: str | None = None
    html_url: str | None = None
    png_url: str | None = None
    html_path: str | None = None
    png_path: str | None = None
    title: str | None = None


@dataclass
class GraphConfig:
    """Configuration for graph generation."""

    chart_type: str
    data_points: list[dict[str, Any]]
    colors: list[str]
    title: str | None = None
