"""Visualization service."""

import json
import logging
from typing import Any

from src.config.prompts import build_viz_prompt
from src.config.settings import Settings
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import azure_agent_client, get_shared_credential
from src.infrastructure.mcp.client import mcp_connection
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
    ) -> dict[str, Any]:
        """
        Generate visualization for SQL results.
        """
        try:
            viz_input = {
                "user_id": user_id,
                "sql_results": {
                    "pregunta_original": question,
                    "sql": sql_query or "",
                    "tablas": tablas or [],
                    "resultados": sql_results,
                    "total_filas": len(sql_results),
                    "resumen": resumen or "",
                },
                "original_question": question,
            }

            input_str = json.dumps(viz_input, ensure_ascii=False)

            system_prompt = build_viz_prompt()
            model = self.settings.viz_agent_model
            viz_max_tokens = self.settings.viz_max_tokens
            viz_temperature = self.settings.viz_temperature

            credential = get_shared_credential()

            async with (
                azure_agent_client(
                    self.settings,
                    model,
                    credential,  # max_iterations=2
                ) as client,
                mcp_connection(self.settings) as mcp,
            ):
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
                return result_model.model_dump()

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
