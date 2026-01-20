"""Visualization service."""

import json
import logging
from typing import Any
from urllib.parse import urlparse

from src.config.prompts import build_viz_prompt
from src.config.settings import Settings
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import azure_agent_client, get_shared_credential
from src.infrastructure.mcp.client import mcp_connection
from src.infrastructure.storage.blob_client import BlobStorageClient
from src.services.viz.models import VizResult

logger = logging.getLogger(__name__)


class VisualizationService:
    """Orchestrates visualization flow."""

    def __init__(self, settings: Settings):
        """Initialize visualization service."""
        self.settings = settings

    async def generate(
        self,
        sql_results: list[Any],
        user_id: str,
        question: str,
        sql_query: str | None = "",
        tablas: list[str] | None = None,
        resumen: str | None = "",
        chart_type: str | None = None,
        mcp: Any | None = None,
    ) -> dict[str, Any]:
        """
        Generate visualization for SQL results.
        """
        try:
            viz_input = {
                "user_id": user_id,
                "sql_results": {
                    "resultados": sql_results,
                },
                "original_question": question,
                "tipo_grafico": chart_type,
            }

            input_str = json.dumps(viz_input, ensure_ascii=False)

            system_prompt = build_viz_prompt()
            model = self.settings.viz_agent_model
            viz_max_tokens = self.settings.viz_max_tokens
            viz_temperature = self.settings.viz_temperature

            credential = get_shared_credential()

            async with azure_agent_client(
                self.settings,
                model,
                credential,  # max_iterations=2
            ) as client:
                if mcp is None:
                    async with mcp_connection(self.settings) as mcp_tool:
                        agent = client.create_agent(
                            name="VisualizationService",
                            instructions=system_prompt,
                            tools=[mcp_tool],
                            max_tokens=viz_max_tokens,
                            temperature=viz_temperature,
                        )

                        result_model = await run_agent_with_format(
                            agent,
                            input_str,
                            response_format=VizResult,
                        )
                else:
                    agent = client.create_agent(
                        name="VisualizationService",
                        instructions=system_prompt,
                        tools=[mcp],
                        max_tokens=viz_max_tokens,
                        temperature=viz_temperature,
                    )

                    result_model = await run_agent_with_format(
                        agent,
                        input_str,
                        response_format=VizResult,
                    )

            if isinstance(result_model, VizResult):
                data = result_model.model_dump()

                # Sign URLs before returning to frontend
                storage_client = BlobStorageClient(self.settings)
                try:
                    container_name = self.settings.azure_storage_container_name or "charts"

                    # Sign image_url if it exists and is a blob URL
                    if data.get("image_url") and "blob.core.windows.net" in data["image_url"]:
                        try:
                            # Remove any existing SAS token from URL before parsing
                            url_to_parse = data["image_url"].split("?")[0]
                            parsed = urlparse(url_to_parse)
                            # Extract blob name from path (format: /container/blob-name)
                            path_parts = parsed.path.strip("/").split("/", 1)
                            if len(path_parts) > 1:
                                blob_name = path_parts[1]
                                data["image_url"] = await storage_client.get_blob_sas_url(
                                    container_name=container_name,
                                    blob_name=blob_name,
                                )
                                logger.debug(f"Signed image_url for blob: {blob_name}")
                            else:
                                logger.warning(f"Could not extract blob name from image_url: {data['image_url']}")
                        except Exception as e:
                            logger.error(f"Error signing image_url: {e}", exc_info=True)

                    # Sign powerbi_url if it exists and is a blob URL
                    if data.get("powerbi_url") and "blob.core.windows.net" in data["powerbi_url"]:
                        try:
                            # Remove any existing SAS token from URL before parsing
                            url_to_parse = data["powerbi_url"].split("?")[0]
                            parsed = urlparse(url_to_parse)
                            # Extract blob name from path (format: /container/blob-name)
                            path_parts = parsed.path.strip("/").split("/", 1)
                            if len(path_parts) > 1:
                                blob_name = path_parts[1]
                                data["powerbi_url"] = await storage_client.get_blob_sas_url(
                                    container_name=container_name,
                                    blob_name=blob_name,
                                )
                                logger.debug(f"Signed powerbi_url for blob: {blob_name}")
                            else:
                                logger.warning(f"Could not extract blob name from powerbi_url: {data['powerbi_url']}")
                        except Exception as e:
                            logger.error(f"Error signing powerbi_url: {e}", exc_info=True)

                except Exception as e:
                    logger.error(f"Error signing viz output URLs: {e}", exc_info=True)
                finally:
                    await storage_client.close()

                return data

            # Fallback if the model does not return a VizResult instance
            return {
                "tipo_grafico": None,
                "metric_name": None,
                "data_points": [],
                "powerbi_url": None,
                "run_id": None,
                "image_url": None,
                "error": "Invalid format",
            }

        except Exception as e:
            logger.error(f"Visualization error: {e}", exc_info=True)
            return {
                "tipo_grafico": None,
                "powerbi_url": None,
                "image_url": None,
                "run_id": None,
                "error": str(e),
            }
