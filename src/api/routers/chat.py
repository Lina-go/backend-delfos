"""Chat and health endpoints."""

import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from src.api.models import ChatRequest, ChatResponse, HealthResponse
from src.config.settings import Settings, get_settings
from src.orchestrator.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """Process a natural language question through the NL2SQL pipeline."""
    try:
        async with PipelineOrchestrator(settings) as orchestrator:
            return await orchestrator.process(request.message, request.user_id)
    except Exception as e:
        logger.error("Error processing chat request: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error") from e


@router.post("/chat/stream", response_class=StreamingResponse)
async def chat_stream(
    request: ChatRequest,
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    """Stream pipeline results as Server-Sent Events."""

    async def generate() -> AsyncIterator[str]:
        try:
            async with PipelineOrchestrator(settings) as orchestrator:
                async for event in orchestrator.process_stream(request.message, request.user_id):
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                logger.info("Stream completed successfully")
        except Exception as e:
            logger.error("Error in streaming: %s", e, exc_info=True)
            yield f"data: {json.dumps({'step': 'error', 'error': 'An error occurred'}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Health check."""
    return HealthResponse(status="healthy", version="0.1.0")
