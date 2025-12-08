"""Visualization service."""

import logging
import json
from typing import Dict, Any, List

from src.config.settings import Settings
from agent_framework import ChatAgent 
from src.infrastructure.llm.executor import run_agent_with_format
from src.infrastructure.llm.factory import azure_agent_client, get_shared_credential
from src.infrastructure.mcp.client import mcp_connection
from src.config.prompts import build_viz_prompt
from src.services.viz.models import VizResult

logger = logging.getLogger(__name__)

class VisualizationService:
    """Orchestrates visualization flow."""

    def __init__(self, settings: Settings):
        """Initialize visualization service."""
        self.settings = settings

    async def generate(
        self,
        sql_results: List[Any],
        user_id: str,
        question: str,
        sql_query: str = "",
    ) -> Dict[str, Any]:
        """
        Generate visualization for SQL results.
        """
        try:
            # Preparar el input incluyendo el SQL query para contexto
            viz_input = {
                "user_id": user_id,
                "sql_results": {
                    "pregunta_original": question,
                    "sql": sql_query,
                    "resultados": sql_results,
                    "total_filas": len(sql_results),
                },
                "original_question": question,
            }
            
            # Serializar a JSON string
            input_str = json.dumps(viz_input, ensure_ascii=False)

            system_prompt = build_viz_prompt()
            model = self.settings.viz_agent_model
            viz_max_tokens = self.settings.viz_max_tokens
            viz_temperature = self.settings.viz_temperature

            credential = get_shared_credential()
            async with azure_agent_client(
                self.settings, model, credential
            ) as client:
                async with mcp_connection(self.settings) as mcp:
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
                        response_format=VizResult
                    )
            
            if isinstance(result_model, VizResult):
                return result_model.model_dump()
            
            logger.warning(f"Resultado inesperado del agente: {type(result_model)}")
            return {
                "tipo_grafico": None,
                "metric_name": None,
                "data_points": [],
                "powerbi_url": None,
                "run_id": None,
                "image_url": None,
                "error": "Formato inválido"
            }

        except Exception as e:
            logger.error(f"Visualization error: {e}", exc_info=True)
            return {
                "tipo_grafico": None,
                "powerbi_url": None,
                "image_url": None,
                "run_id": None,
                "error": str(e)
            }