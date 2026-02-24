"""Advisor chat endpoints for informe financial analysis."""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from src.api.models import (
    AdvisorChatRequest,
    AdvisorChatResponse,
    ProactiveInsightsRequest,
    ProactiveInsightsResponse,
)
from src.config.settings import Settings, get_settings
from src.infrastructure.database.connection import execute_wh_query
from src.services.advisor.agent import AdvisorAgent
from src.services.informes import InformeService

logger = logging.getLogger(__name__)

router = APIRouter()


def _parse_graph_content(raw: str) -> Any:
    """Parse graph content from JSON string into a dict so the LLM receives structured data."""
    if not raw or not raw.strip():
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw


def _make_json_safe(value: Any) -> Any:
    """Convert Decimal/date/datetime/bytes values to JSON-serializable types."""
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


# Total data_points budget across ALL graphs to stay within the LLM token window.
# Divided equally among graphs so more graphs = fewer rows each.
# Keep low to avoid 429 rate-limit (Azure OpenAI 50K token budget).
_TOTAL_DATA_POINTS_BUDGET = 100

# ---------------------------------------------------------------------------
# Report context cache — keyed by informe_id, shared across users.
# ---------------------------------------------------------------------------
_CONTEXT_CACHE_TTL = 300  # 5 minutes
_context_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _invalidate_context_cache(informe_id: str) -> None:
    """Remove a cached report context so the next request rebuilds it."""
    _context_cache.pop(informe_id, None)


async def _fetch_graph_data(settings: Settings, query: str) -> list[dict[str, Any]] | None:
    """Execute a graph's stored SQL query against the warehouse."""
    try:
        rows = await execute_wh_query(settings, query)
        return [
            {k: _make_json_safe(v) for k, v in row.items()}
            for row in rows
        ]
    except Exception as e:
        logger.warning("Failed to fetch graph data: %s", e)
        return None


def _truncate_data_points(data: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Keep only the last *limit* data_points to stay within token budget."""
    if len(data) <= limit:
        return data
    return data[-limit:]


async def _build_report_context(informe_id: str, settings: Settings) -> dict[str, Any]:
    """Build the report_context dict from scratch (DB + warehouse).

    Graph data queries that need fetching are executed in parallel via
    ``asyncio.gather`` instead of sequentially.
    """
    svc = InformeService(settings)
    informe = await svc.get_informe(informe_id)

    n_graphs = max(len(informe.graphs), 1)
    per_graph_limit = _TOTAL_DATA_POINTS_BUDGET // n_graphs

    # --- Phase 1: parse content & identify graphs that need warehouse fetch ---
    parsed_list: list[tuple[Any, bool]] = []  # (parsed_content, has_data_points)
    fetch_indices: list[int] = []
    fetch_queries: list[str] = []

    for i, g in enumerate(informe.graphs):
        parsed = _parse_graph_content(g.content)
        has_dp = isinstance(parsed, dict) and "data_points" in parsed

        if not has_dp and g.query:
            fetch_indices.append(i)
            fetch_queries.append(g.query)
        elif has_dp and isinstance(parsed.get("data_points"), list):
            parsed["data_points"] = _truncate_data_points(parsed["data_points"], per_graph_limit)

        parsed_list.append((parsed, has_dp))

    # --- Phase 2: fetch missing data_points in parallel ---
    if fetch_queries:
        results = await asyncio.gather(
            *[_fetch_graph_data(settings, q) for q in fetch_queries],
            return_exceptions=True,
        )
        for idx, result in zip(fetch_indices, results):
            if isinstance(result, list) and result:
                parsed_list[idx] = (
                    {"data_points": _truncate_data_points(result, per_graph_limit)},
                    True,
                )

    # --- Phase 3: assemble final context ---
    graphs = []
    for i, g in enumerate(informe.graphs):
        parsed, has_dp = parsed_list[i]
        graphs.append({
            "graph_id": g.graph_id,
            "title": g.title,
            "type": g.type,
            "label_name": g.label_name,
            "content": parsed,
            "has_data_points": has_dp,
        })

    return {
        "informe_id": informe.id,
        "title": informe.title,
        "description": informe.description,
        "owner": informe.owner,
        "labels": [
            {"id": lbl.id, "name": lbl.name, "chart_count": lbl.chart_count}
            for lbl in informe.labels
        ],
        "graphs": graphs,
    }


async def _get_report_context(informe_id: str, settings: Settings) -> dict[str, Any]:
    """Return report context, using an in-memory cache with TTL.

    The report context is the same for all users viewing the same informe,
    so caching by ``informe_id`` avoids redundant DB + warehouse queries
    on every chat message.
    """
    cached = _context_cache.get(informe_id)
    if cached is not None:
        ts, ctx = cached
        if time.time() - ts < _CONTEXT_CACHE_TTL:
            logger.debug("Report context cache hit for informe %s", informe_id)
            return ctx

    ctx = await _build_report_context(informe_id, settings)
    _context_cache[informe_id] = (time.time(), ctx)
    logger.debug("Report context cached for informe %s", informe_id)
    return ctx


@router.post("/chat", response_model=AdvisorChatResponse)
async def advisor_chat(
    request: AdvisorChatRequest,
    settings: Settings = Depends(get_settings),
) -> AdvisorChatResponse:
    """Send a message to the financial advisor agent."""
    try:
        report_context = await _get_report_context(request.informe_id, settings)
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
        report_context = await _get_report_context(request.informe_id, settings)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Stream setup error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Error initializing stream") from e

    async def generate() -> AsyncIterator[str]:
        try:
            agent = AdvisorAgent(settings)
            async for chunk in agent.chat_stream(
                user_id=request.user_id,
                informe_id=request.informe_id,
                message=request.message,
                report_context=report_context,
            ):
                yield f"data: {json.dumps({'text': chunk}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
            logger.info("Advisor stream completed successfully")
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
    """Generate proactive insights when a user opens an informe.

    Temporarily disabled — returns a welcome message instead of calling the agent,
    since graph content may not always contain data_points for analysis.
    """
    return ProactiveInsightsResponse(
        insights="Hola, soy tu Advisor Financiero. Preguntame sobre los datos de este informe.",
        informe_id=request.informe_id,
    )


@router.delete("/session")
async def clear_session(
    user_id: str,
    informe_id: str,
    settings: Settings = Depends(get_settings),
) -> dict[str, str]:
    """Clear advisor session for a user + informe pair (memory + DB)."""
    await AdvisorAgent.clear_session(settings, user_id, informe_id)
    _invalidate_context_cache(informe_id)
    return {"status": "cleared"}
