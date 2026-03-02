"""Advisor chat endpoints for informe financial analysis."""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from src.api.models import (
    AdvisorChatRequest,
    AdvisorChatResponse,
    InsightItem,
    ProactiveInsightsRequest,
    ProactiveInsightsResponse,
)
from src.config.settings import Settings, get_settings
from src.services.advisor.agent import AdvisorAgent
from src.services.advisor.report_context import get_report_context, invalidate_context_cache

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/chat", response_model=AdvisorChatResponse)
async def advisor_chat(
    request: AdvisorChatRequest,
    settings: Settings = Depends(get_settings),
) -> AdvisorChatResponse:
    """Send a message to the financial advisor agent."""
    try:
        report_context = await get_report_context(request.informe_id, settings)
        agent = AdvisorAgent(settings)
        response = await agent.chat(
            user_id=request.user_id,
            informe_id=request.informe_id,
            message=request.message,
            report_context=report_context,
        )
        return AdvisorChatResponse(
            response=response, informe_id=request.informe_id
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Advisor chat error: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500, detail="Error processing advisor request"
        ) from e


@router.post("/chat/stream", response_class=StreamingResponse)
async def advisor_chat_stream(
    request: AdvisorChatRequest,
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    """Stream advisor response as Server-Sent Events."""
    try:
        report_context = await get_report_context(request.informe_id, settings)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Stream setup error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Error initializing stream") from e

    _ADVISOR_STREAM_TIMEOUT = 180  # 3 minutes max

    async def generate() -> AsyncIterator[str]:
        try:
            t_start = time.time()
            agent = AdvisorAgent(settings)
            chunk_count = 0
            t_first_chunk = None
            async with asyncio.timeout(_ADVISOR_STREAM_TIMEOUT):
                async for chunk in agent.chat_stream(
                    user_id=request.user_id,
                    informe_id=request.informe_id,
                    message=request.message,
                    report_context=report_context,
                ):
                    chunk_count += 1
                    if t_first_chunk is None:
                        t_first_chunk = time.time() - t_start
                        logger.info("[ADVISOR TIMING] First chunk: %.2fs", t_first_chunk)
                    yield f"data: {json.dumps({'text': chunk}, ensure_ascii=False)}\n\n"
            t_total = time.time() - t_start
            logger.info(
                "[ADVISOR TIMING] Stream complete: %.2fs total, %d chunks, first_chunk=%.2fs",
                t_total, chunk_count, t_first_chunk or 0,
            )
            yield f"data: {json.dumps({'done': True})}\n\n"
        except TimeoutError:
            logger.error("[ADVISOR TIMING] Stream timeout after %ds", _ADVISOR_STREAM_TIMEOUT)
            yield f"data: {json.dumps({'error': 'La solicitud tardó demasiado. Intenta de nuevo.'})}\n\n"
        except Exception as e:
            logger.error("Advisor stream error: %s", e, exc_info=True)
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/insights/proactive", response_model=ProactiveInsightsResponse)
async def advisor_proactive_insights(
    request: ProactiveInsightsRequest,
    settings: Settings = Depends(get_settings),
) -> ProactiveInsightsResponse:
    """Generate top-3 proactive insights when a user opens an informe."""
    report_context = await get_report_context(request.informe_id, settings)
    agent = AdvisorAgent(settings)
    raw_insights = await agent.generate_proactive_insights(
        user_id=request.user_id,
        informe_id=request.informe_id,
        report_context=report_context,
    )
    insights = [InsightItem(**item) for item in raw_insights]
    return ProactiveInsightsResponse(insights=insights, informe_id=request.informe_id)


@router.delete("/session/{user_id}/{informe_id}")
async def clear_session(
    user_id: str,
    informe_id: str,
    settings: Settings = Depends(get_settings),
) -> dict[str, str]:
    """Clear advisor session for a user + informe pair (memory + DB)."""
    await AdvisorAgent.clear_session(settings, user_id, informe_id)
    invalidate_context_cache(informe_id)
    return {"status": "cleared"}
