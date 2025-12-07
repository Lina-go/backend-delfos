"""Visualization service."""

import logging
from typing import Dict, Any, List

from src.config.settings import Settings
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
        sql_results: List[Any],  # Can be List[str] from MCP or List[Dict] after parsing
        user_id: str,
        question: str,
    ) -> Dict[str, Any]:
        """
        Generate visualization for SQL results.
        
        The agent handles chart selection, data formatting, and MCP tool calls
        to generate Power BI URLs. This service only orchestrates the agent execution.
        
        Args:
            sql_results: SQL query results
            user_id: User identifier
            question: Original user question
            
        Returns:
            Dictionary with visualization data (from agent response)
        """
        try:
            viz_input = {
                "user_id": user_id,
                "sql_results": {
                    "pregunta_original": question,
                    "resultados": sql_results,
                    "total_filas": len(sql_results),
                },
                "original_question": question,
            }

            system_prompt = build_viz_prompt()
            model = self.settings.viz_agent_model
            viz_max_tokens = self.settings.viz_max_tokens
            viz_temperature = self.settings.viz_temperature

            # Create agent with MCP tools
            credential = get_shared_credential()
            async with azure_agent_client(
                self.settings, model, credential
            ) as client:
                async with mcp_connection(self.settings) as mcp:
                    agent = client.create_agent(
                        name="VisualizationService",
                        instructions=system_prompt,
                        tools=mcp,
                        max_tokens=viz_max_tokens,
                        temperature=viz_temperature,
                    )
                    result_model = await run_agent_with_format(
                        agent, str(viz_input), response_format=VizResult
                    )
            
            # Convert Pydantic model to dict
            if isinstance(result_model, VizResult):
                return result_model.model_dump()
            else:
                # Fallback if result is not the expected type
                return {
                    "tipo_grafico": None,
                    "metric_name": None,
                    "data_points": [],
                    "powerbi_url": None,
                    "run_id": None,
                    "image_url": None,
                }

        except Exception as e:
            logger.error(f"Visualization error: {e}", exc_info=True)
            return {
                "tipo_grafico": None,
                "powerbi_url": None,
                "image_url": None,
                "run_id": None,
            }

