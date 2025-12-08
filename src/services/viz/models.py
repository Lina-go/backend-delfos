"""Visualization service models."""

from dataclasses import dataclass
from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class VizResult(BaseModel):
    """Result from visualization generation."""

    tipo_grafico: Optional[str] = None
    metric_name: Optional[str] = None
    data_points: Optional[List[Dict[str, Any]]] = None
    powerbi_url: Optional[str] = None
    run_id: Optional[str] = None
    image_url: Optional[str] = None


@dataclass
class ChartConfig:
    """Configuration for chart generation."""

    chart_type: str
    data_points: List[Dict[str, Any]]
    title: Optional[str] = None
    x_label: Optional[str] = None
    y_label: Optional[str] = None

