"""API routes."""

import logging
from fastapi import APIRouter, Depends, HTTPException

from src.api.models import ChatRequest, ChatResponse, HealthResponse, SchemaResponse
from src.api.dependencies import get_settings_dependency
from src.config.settings import Settings
from src.orchestrator.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    settings: Settings = Depends(get_settings_dependency),
):
    """
    Main chat endpoint for natural language to SQL pipeline.
    
    Processes the user's question through the complete pipeline:
    1. Triage
    2. Intent classification
    3. Schema selection
    4. SQL generation & execution
    5. Verification
    6. Visualization (if needed)
    7. Response formatting
    """
    try:
        orchestrator = PipelineOrchestrator(settings)
        response = await orchestrator.process(request.message, request.user_id)
        return response
    except Exception as e:
        logger.error(f"Error processing chat request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(status="healthy", version="0.1.0")


@router.get("/schema", response_model=SchemaResponse)
async def get_schema(
    settings: Settings = Depends(get_settings_dependency),
):
    """Get database schema information."""
    try:
        # TODO: Implement schema retrieval via MCP
        return SchemaResponse(tables=[])
    except Exception as e:
        logger.error(f"Error retrieving schema: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/graph/{run_id}")
async def get_graph(
    run_id: str,
    settings: Settings = Depends(get_settings_dependency),
):
    """
    Get graph image URL by run_id.
    
    Args:
        run_id: Run ID from VizAgent
        
    Returns:
        Dictionary with image_url
    """
    try:
        from src.services.graph.service import GraphService
        
        # TODO: Retrieve graph data from storage/cache using run_id
        # For now, return a placeholder
        return {
            "run_id": run_id,
            "image_url": None,
            "status": "not_found",
        }
    except Exception as e:
        logger.error(f"Error retrieving graph: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

