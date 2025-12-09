"""API routes."""

import json
import logging
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from src.api.models import ChatRequest, ChatResponse, HealthResponse, SchemaResponse
from src.api.dependencies import get_settings_dependency
from src.config.settings import Settings
from src.infrastructure.cache.semantic_cache import SemanticCache
from src.orchestrator.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/chat", response_model=ChatResponse, tags=["chat"])
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


@router.post(
    "/chat/stream",
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Server-Sent Events stream",
            "content": {
                "text/event-stream": {
                    "schema": {
                        "type": "string",
                        "example": 'data: {"step": "triage", "result": {...}, "state": {...}}\n\n',
                    }
                }
            },
        }
    },
    tags=["chat"],
)
async def chat_stream(
    request: ChatRequest,
    settings: Settings = Depends(get_settings_dependency),
):
    """
    Streaming chat endpoint using Server-Sent Events (SSE).
    
    Emits events as each pipeline step completes:
    - triage: Triage classification result
    - intent: Intent classification result
    - schema: Schema selection result
    - sql_generation: SQL generation result
    - sql_execution: SQL execution result
    - verification: Verification result
    - visualization: Visualization result (if required)
    - graph: Graph generation result (if visualization)
    - format: Response formatting result
    - complete: Final response
    - error: Error event (if any)
    
    **Note**: This endpoint returns a streaming response. Use EventSource or fetch with streaming
    to consume the events in real-time.
    """
    async def generate():
        orchestrator = None
        try:
            orchestrator = PipelineOrchestrator(settings)
            async for event in orchestrator.process_stream(request.message, request.user_id):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            logger.info("Stream completed successfully")
        except Exception as e:
            logger.error(f"Error in streaming: {e}", exc_info=True)
            yield f"data: {json.dumps({'step': 'error', 'error': str(e)}, ensure_ascii=False)}\n\n"
        finally:
            if orchestrator:
                try:
                    await orchestrator.close()
                    logger.info("Stream connection closed")
                except Exception as cleanup_error:
                    logger.warning(f"Error during cleanup: {cleanup_error}")
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@router.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(status="healthy", version="0.1.0")


@router.get("/cache/stats", tags=["cache"])
async def get_cache_stats():
    """
    Get cache statistics.
    
    Returns:
        Dictionary with cache statistics:
        - size: Number of cached entries
        - hits: Number of cache hits
        - misses: Number of cache misses
        - hit_rate: Hit rate as percentage
    """
    stats = SemanticCache.get_stats()
    return stats


@router.delete("/cache", tags=["cache"])
async def clear_cache():
    """
    Clear all cached SQL generation results.
    
    Use this endpoint when you want to invalidate the cache,
    for example after schema changes or when you want fresh results.
    
    Returns:
        Success message
    """
    SemanticCache.clear()
    logger.info("Cache cleared by API request")
    return {"message": "Cache cleared successfully", "status": "success"}


