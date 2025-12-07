"""Visualization service."""

import logging
from typing import Dict, Any, List, Optional
from azure.identity.aio import DefaultAzureCredential

from src.config.settings import Settings
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import azure_agent_client
from src.infrastructure.mcp.client import mcp_connection
from src.config.prompts import build_viz_prompt
from src.utils.json_parser import JSONParser
from src.services.viz.models import VizResult



logger = logging.getLogger(__name__)


class VisualizationService:
    """Orchestrates visualization flow."""

    def __init__(self, settings: Settings):
        """Initialize visualization service."""
        self.settings = settings
        self._credential: Optional[DefaultAzureCredential] = None

    async def generate(
        self,
        sql_results: List[Dict[str, Any]],
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
            # Get or create credential for Azure
            if not self._credential:
                self._credential = DefaultAzureCredential()

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
            viz_response_format = VizResult

            # Create agent with MCP tools
            async with azure_agent_client(
                self.settings, model, self._credential
            ) as client:
                async with mcp_connection(self.settings) as mcp:
                    agent = client.create_agent(
                        name="VisualizationService",
                        instructions=system_prompt,
                        tools=mcp,
                        max_tokens=viz_max_tokens,
                        temperature=viz_temperature,
                        response_format=viz_response_format
                    )
                    response = await run_single_agent(agent, str(viz_input))

            # Parse and return the agent's response
            viz_result = JSONParser.extract_json(response)
            return viz_result

        except Exception as e:
            logger.error(f"Visualization error: {e}", exc_info=True)
            return {
                "tipo_grafico": None,
                "powerbi_url": None,
                "image_url": None,
                "run_id": None,
            }

