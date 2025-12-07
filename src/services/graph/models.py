"""Graph service models."""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass
class GraphResult:
    """Result from graph generation."""

    image_url: str
    chart_type: str
    run_id: Optional[str] = None
    blob_url: Optional[str] = None


@dataclass
class GraphConfig:
    """Configuration for graph generation."""

    chart_type: str
    data_points: List[Dict[str, Any]]
    colors: List[str]
    title: Optional[str] = None

