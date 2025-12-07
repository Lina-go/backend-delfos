"""Graph service orchestrator."""

import logging
from typing import Dict, Any, List

from src.config.settings import Settings
from src.services.graph.executor import GraphExecutor
from src.services.graph.models import GraphResult

logger = logging.getLogger(__name__)


class GraphService:
    """Orchestrates graph generation flow."""

    def __init__(self, settings: Settings):
        """Initialize graph service."""
        self.settings = settings
        self.executor = GraphExecutor(settings)

    async def generate(
        self,
        run_id: str,
        chart_type: str,
        data_points: List[Dict[str, Any]],
    ) -> GraphResult:
        """
        Generate graph image.
        
        Args:
            run_id: Run ID from VizAgent
            chart_type: Chart type
            data_points: Data points
            
        Returns:
            GraphResult with image URL
        """
        try:
            result = await self.executor.generate_graph(
                run_id=run_id,
                chart_type=chart_type,
                data_points=data_points,
            )
            
            return GraphResult(
                image_url=result["image_url"],
                chart_type=chart_type,
                run_id=run_id,
            )

        except Exception as e:
            logger.error(f"Graph service error: {e}", exc_info=True)
            raise

