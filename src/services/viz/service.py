"""Visualization service."""

import json
import logging
from typing import Dict, Any, List, Optional

from src.config.settings import Settings
from src.infrastructure.llm.executor import run_single_agent
from src.infrastructure.llm.factory import azure_agent_client, get_shared_credential
from src.infrastructure.mcp.client import mcp_connection
from src.config.prompts import build_viz_prompt
from src.utils.json_parser import JSONParser



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
        sql_query: Optional[str] = None,
        tablas: Optional[List[str]] = None,
        resumen: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate visualization for SQL results.
        
        The agent handles chart selection, data formatting, and MCP tool calls
        to generate Power BI URLs. This service only orchestrates the agent execution.
        
        Args:
            sql_results: SQL query results
            user_id: User identifier
            question: Original user question
            sql_query: The SQL query that was executed
            tablas: List of table names used in the query
            resumen: Summary of the results
            
        Returns:
            Dictionary with visualization data (from agent response)
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

            system_prompt = build_viz_prompt()
            model = self.settings.viz_agent_model
            viz_max_tokens = self.settings.viz_max_tokens
            viz_temperature = self.settings.viz_temperature

            # Create agent with restricted MCP tools
            # VisualizationService only needs these 2 tools: insert_agent_output_batch + generate_powerbi_url
            viz_tools = [
                "insert_agent_output_batch",
                "generate_powerbi_url",
            ]
            
            credential = get_shared_credential()
            async with azure_agent_client(
                self.settings, model, credential, max_iterations=3
            ) as client:
                async with mcp_connection(self.settings, allowed_tools=viz_tools) as mcp:
                    agent = client.create_agent(
                        name="VisualizationService",
                        instructions=system_prompt,
                        tools=mcp,
                        max_tokens=viz_max_tokens,
                        temperature=viz_temperature,
                    )
                    input_json = json.dumps(viz_input, ensure_ascii=False, indent=2)
                    logger.info(f"VisualizationService input JSON: {input_json[:1000]}...")  # Log first 1000 chars
                    
                    # Use run_single_agent like in workflow.py (no response_format)
                    raw_viz_result = await run_single_agent(agent, input_json)
                    logger.info(f"VisualizationService raw result (length: {len(raw_viz_result)}): {raw_viz_result[:500]}...")
                    
                    # Extract JSON manually like in workflow.py
                    viz_json = JSONParser.extract_json(raw_viz_result)
                    logger.info(f"VisualizationService extracted JSON: {json.dumps(viz_json, indent=2, ensure_ascii=False) if viz_json else 'None'}")
                    
                    if viz_json:
                        # Ensure all fields have defaults
                        viz_json.setdefault("tipo_grafico", None)
                        viz_json.setdefault("metric_name", None)
                        viz_json.setdefault("data_points", [])
                        viz_json.setdefault("powerbi_url", None)
                        viz_json.setdefault("run_id", None)
                        viz_json.setdefault("image_url", None)
                        return viz_json
                    else:
                        logger.warning("VisualizationService: Could not extract JSON from agent response")
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

