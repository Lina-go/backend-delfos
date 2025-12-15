"""Graph executor agent."""

import logging
from pathlib import Path
from typing import Any

from src.config.settings import Settings
from src.services.graph.tools import (
    generate_bar_chart,
    generate_line_chart,
    generate_pie_chart,
    generate_stacked_bar_chart,
)

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"


class GraphExecutor:
    """Executes graph generation using local tools."""

    def __init__(self, settings: Settings):
        """Initialize graph executor."""
        self.settings = settings

    async def generate_graph(
        self,
        run_id: str,
        chart_type: str,
        data_points: list[dict[str, Any]],
        title: str,
    ) -> dict[str, Any]:
        """
        Generate graph HTML/PNG using local tools and upload to blob storage.
        """
        try:
            chart_functions = {
                "pie": generate_pie_chart,
                "bar": generate_bar_chart,
                "line": generate_line_chart,
                "stackedbar": generate_stacked_bar_chart,
            }

            if chart_type not in chart_functions:
                raise ValueError(f"Unknown chart type: {chart_type}")

            chart_func = chart_functions[chart_type]
            artifacts = chart_func(data_points=data_points, title=title)
            html_bytes = artifacts["html_bytes"]
            png_bytes = artifacts["png_bytes"]

            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            html_path = OUTPUT_DIR / f"{run_id}_{chart_type}.html"
            png_path = OUTPUT_DIR / f"{run_id}_{chart_type}.png"
            html_path.write_bytes(html_bytes)
            png_path.write_bytes(png_bytes)

            from src.infrastructure.storage.blob_client import BlobStorageClient

            storage = BlobStorageClient(self.settings)
            container = self.settings.azure_storage_container_name or "charts"
            html_blob = f"{run_id}_{chart_type}.html"
            png_blob = f"{run_id}_{chart_type}.png"

            html_url: str | None = None
            png_url: str | None = None

            try:
                try:
                    html_url = await storage.upload_blob(
                        container_name=container,
                        blob_name=html_blob,
                        data=html_bytes,
                        content_type="text/html",
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("HTML upload skipped or failed: %s", exc)

                try:
                    png_url = await storage.upload_blob(
                        container_name=container,
                        blob_name=png_blob,
                        data=png_bytes,
                        content_type="image/png",
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("PNG upload skipped or failed: %s", exc)

                return {
                    "image_url": png_url,
                    "html_url": html_url,
                    "png_url": png_url,
                    "html_path": str(html_path),
                    "png_path": str(png_path),
                    "run_id": run_id,
                }
            finally:
                await storage.close()

        except Exception as e:
            logger.error(f"Graph generation error: {e}", exc_info=True)
            raise
