"""Graph executor agent."""

import base64
import logging
from typing import Dict, Any, List

from src.config.settings import Settings
from src.services.graph.tools import (
    generate_pie_chart,
    generate_bar_chart,
    generate_line_chart,
    generate_stacked_bar_chart,
)

logger = logging.getLogger(__name__)


class GraphExecutor:
    """Executes graph generation using local tools."""

    def __init__(self, settings: Settings):
        """Initialize graph executor."""
        self.settings = settings

    async def generate_graph(
        self,
        run_id: str,
        chart_type: str,
        data_points: List[Dict[str, Any]],
        title: str,
    ) -> Dict[str, Any]:
        """
        Generate graph image using local tools.
        
        Args:
            run_id: Run ID from VizAgent
            chart_type: Chart type (pie, bar, line, stackedbar)
            data_points: Data points for the chart
            title: Title of the graph
        Returns:
            Dictionary with image_url
        """
        try:
            # Map chart type to function
            chart_functions = {
                "pie": generate_pie_chart,
                "bar": generate_bar_chart,
                "line": generate_line_chart,
                "stackedbar": generate_stacked_bar_chart,
            }
            
            if chart_type not in chart_functions:
                raise ValueError(f"Unknown chart type: {chart_type}")
            
            # Generate chart
            chart_func = chart_functions[chart_type]
            image_base64 = chart_func(data_points)
            
            # Upload to blob storage
            from src.infrastructure.storage.blob_client import BlobStorageClient
            storage = BlobStorageClient(self.settings)
            blob_name = f"{run_id}_{chart_type}.png"
            
            # Decode base64 to bytes
            if isinstance(image_base64, str):
                image_bytes = base64.b64decode(image_base64)
            else:
                image_bytes = image_base64
            
            image_url = await storage.upload_blob(
                container_name=self.settings.azure_storage_container_name,
                blob_name=blob_name,
                data=image_bytes,
                content_type="image/png",
            )
            
            return {
                "image_url": image_url,
                "run_id": run_id,
            }

        except Exception as e:
            logger.error(f"Graph generation error: {e}", exc_info=True)
            raise

