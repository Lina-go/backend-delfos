"""Visualization service."""

import json
import logging
from typing import Any
from urllib.parse import urlparse

from src.config.prompts import build_viz_prompt
from src.config.settings import Settings
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import azure_agent_client, get_shared_credential
from src.infrastructure.mcp.client import MCPClient
from src.infrastructure.storage.blob_client import BlobStorageClient
from src.services.viz.models import VizFormattingResult

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
    ) -> dict[str, Any]:
        """Generate visualization for SQL results."""
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
                credential,
            ) as client:
                agent = client.create_agent(
                    name="VizFormattingAgent",
                    instructions=system_prompt,
                    tools=[],
                    max_tokens=viz_max_tokens,
                    temperature=viz_temperature,
                )

                formatting_result = await run_agent_with_format(
                    agent,
                    input_str,
                    response_format=VizFormattingResult,
                )

            if not isinstance(formatting_result, VizFormattingResult):
                return self._error_result("Agent failed to format data")

            logger.info(f"Agent formatted {len(formatting_result.data_points)} data points")

            run_id = None
            powerbi_url = None

            async with MCPClient(self.settings) as mcp_client:
                run_id = await mcp_client.insert_agent_output_batch(
                    user_id=user_id,
                    question=question,
                    results=formatting_result.data_points,
                    metric_name=formatting_result.metric_name,
                    visual_hint=chart_type or "barras",
                )
                logger.info(f"insert_agent_output_batch returned run_id: {run_id}")

                if run_id:
                    powerbi_url = await mcp_client.generate_powerbi_url(
                        run_id=run_id,
                        visual_hint=chart_type or "barras",
                    )
                    logger.info(f"generate_powerbi_url returned URL")

            data = {
                "tipo_grafico": chart_type,
                "metric_name": formatting_result.metric_name,
                "data_points": formatting_result.data_points,
                "powerbi_url": powerbi_url,
                "run_id": run_id,
                "image_url": None,
            }

            storage_client = BlobStorageClient(self.settings)
            try:
                container_name = self.settings.azure_storage_container_name or "charts"

                if data.get("powerbi_url") and "blob.core.windows.net" in data["powerbi_url"]:
                    try:
                        url_to_parse = data["powerbi_url"].split("?")[0]
                        parsed = urlparse(url_to_parse)
                        path_parts = parsed.path.strip("/").split("/", 1)
                        if len(path_parts) > 1:
                            blob_name = path_parts[1]
                            data["powerbi_url"] = await storage_client.get_blob_sas_url(
                                container_name=container_name,
                                blob_name=blob_name,
                            )
                            logger.debug(f"Signed powerbi_url for blob: {blob_name}")
                    except Exception as e:
                        logger.error(f"Error signing powerbi_url: {e}", exc_info=True)

            except Exception as e:
                logger.error(f"Error signing viz output URLs: {e}", exc_info=True)
            finally:
                await storage_client.close()

            return data

        except Exception as e:
            logger.error(f"Visualization error: {e}", exc_info=True)
            return self._error_result(str(e))

    def _error_result(self, error_message: str) -> dict[str, Any]:
        """Build error result dictionary."""
        return {
            "tipo_grafico": None,
            "metric_name": None,
            "data_points": [],
            "powerbi_url": None,
            "run_id": None,
            "image_url": None,
            "error": error_message,
        }