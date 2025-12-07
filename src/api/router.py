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
    orchestrator = None
    try:
        orchestrator = PipelineOrchestrator(settings)
        response = await orchestrator.process(request.message, request.user_id)
        return response
    except Exception as e:
        logger.error(f"Error processing chat request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Cleanup resources
        if orchestrator:
            try:
                await orchestrator.close()
            except Exception as cleanup_error:
                logger.warning(f"Error during cleanup: {cleanup_error}")


@router.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(status="healthy", version="0.1.0")


