"""Chat V2 endpoints — single-agent architecture with workflow tool."""

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from src.api.models import ChatRequest
from src.config.settings import Settings, get_settings
from src.services.chat_v2.agent import ChatV2Agent

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/chat")
async def chat_v2(
    request: ChatRequest,
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """Process a message through the Chat V2 single-agent pipeline.

    The agent's execute_and_visualize tool embeds a JSON block in the response
    with visualization data, data_points, and Power BI link.
    This endpoint extracts that JSON and returns it as structured response.
    """
    try:
        agent = ChatV2Agent(settings)
        response_text = await agent.chat(request.user_id, request.message)

        # The agent's response contains embedded JSON from execute_and_visualize
        viz_data = _extract_viz_json(response_text)

        if viz_data:
            # Clean the JSON block from the text for the insight field
            insight = _clean_json_from_text(response_text, viz_data)

            return {
                "patron": "chat_v2",
                "insight": insight or viz_data.get("titulo_grafica", ""),
                "datos": viz_data.get("datos"),
                "data_points": viz_data.get("data_points"),
                "visualizacion": viz_data.get("visualizacion", "YES"),
                "tipo_grafica": viz_data.get("tipo_grafica"),
                "titulo_grafica": viz_data.get("titulo_grafica"),
                "metric_name": viz_data.get("metric_name"),
                "x_axis_name": viz_data.get("x_axis_name"),
                "y_axis_name": viz_data.get("y_axis_name"),
                "series_name": viz_data.get("series_name"),
                "category_name": viz_data.get("category_name"),
                "is_tasa": viz_data.get("is_tasa", False),
                "link_power_bi": viz_data.get("link_power_bi"),
                "sql_query": viz_data.get("sql_query"),
            }

        # No visualization data — conversational response (greeting, clarification, etc.)
        return {
            "patron": "chat_v2",
            "insight": response_text,
            "visualizacion": "NO",
        }

    except Exception as e:
        logger.error("Error in chat_v2: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error") from e


@router.post("/chat/stream", response_class=StreamingResponse)
async def chat_v2_stream(
    request: ChatRequest,
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    """Stream Chat V2 response as Server-Sent Events.

    Text chunks are emitted as ``{"text": "..."}`` events.
    After all text, a guaranteed ``{"viz_data": {...}}`` event is emitted
    if the execute_and_visualize tool produced visualization data.
    Finally ``{"done": true}`` signals the end.
    """

    _VIZ_SENTINEL = "__VIZ_DATA__"
    _CLARIFICATION_SENTINEL = "__CLARIFICATION__"

    async def generate() -> AsyncIterator[str]:
        try:
            t_start = time.time()
            agent = ChatV2Agent(settings)
            chunk_count = 0
            total_chars = 0
            t_first_chunk = None
            async for chunk in agent.chat_stream(request.user_id, request.message):
                # Sentinel-prefixed chunks: viz data or clarification
                if chunk.startswith(_VIZ_SENTINEL):
                    viz_json_str = chunk[len(_VIZ_SENTINEL):]
                    logger.info("[STREAM] Emitting viz_data event (%d chars)", len(viz_json_str))
                    yield f"data: {json.dumps({'viz_data': json.loads(viz_json_str)}, ensure_ascii=False)}\n\n"
                    continue
                if chunk.startswith(_CLARIFICATION_SENTINEL):
                    clarif_json_str = chunk[len(_CLARIFICATION_SENTINEL):]
                    logger.info("[STREAM] Emitting clarification event")
                    yield f"data: {json.dumps({'clarification': json.loads(clarif_json_str)}, ensure_ascii=False)}\n\n"
                    continue
                chunk_count += 1
                total_chars += len(chunk)
                if t_first_chunk is None:
                    t_first_chunk = time.time() - t_start
                    logger.info("[TIMING] Stream first chunk: %.2fs", t_first_chunk)
                yield f"data: {json.dumps({'text': chunk}, ensure_ascii=False)}\n\n"
            t_total = time.time() - t_start
            logger.info(
                "[TIMING] Stream complete: %.2fs total, %d chunks, %d chars, first_chunk=%.2fs",
                t_total, chunk_count, total_chars, t_first_chunk or 0,
            )
            yield f"data: {json.dumps({'done': True}, ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.error("Error in chat_v2 stream: %s", e, exc_info=True)
            yield f"data: {json.dumps({'error': 'An error occurred'}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.delete("/session/{user_id}")
async def clear_session(
    user_id: str,
    settings: Settings = Depends(get_settings),
) -> dict[str, str]:
    """Clear a user's chat session."""
    await ChatV2Agent.clear_session(settings, user_id)
    return {"status": "ok", "message": f"Session cleared for {user_id}"}


# ---------------------------------------------------------------------------
# JSON extraction helpers (balanced-brace parsing, no fragile regex)
# ---------------------------------------------------------------------------


def _extract_viz_json(text: str) -> dict[str, Any] | None:
    """Extract the visualization JSON embedded in the agent's response.

    The execute_and_visualize tool returns a JSON object with "visualization": true
    and "data_points": [...]. Uses balanced-brace parsing to handle nested JSON.
    """
    # Look for our marker key
    for marker in ('"visualization"', '"visualizacion"'):
        idx = text.find(marker)
        if idx == -1:
            continue
        # Walk backwards to the opening brace
        start = text.rfind("{", 0, idx)
        if start == -1:
            continue
        json_str = _extract_balanced_json(text, start)
        if json_str:
            try:
                parsed = json.loads(json_str)
                if isinstance(parsed, dict) and (
                    parsed.get("visualization") or parsed.get("data_points")
                ):
                    return parsed
            except json.JSONDecodeError:
                pass

    return None


def _extract_balanced_json(text: str, start: int) -> str | None:
    """Extract a balanced JSON object starting at position `start`."""
    if start >= len(text) or text[start] != "{":
        return None

    depth = 0
    in_string = False
    escape = False

    for i in range(start, len(text)):
        ch = text[i]

        if escape:
            escape = False
            continue

        if ch == "\\" and in_string:
            escape = True
            continue

        if ch == '"':
            in_string = not in_string
            continue

        if in_string:
            continue

        if ch in "{[":
            depth += 1
        elif ch in "}]":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]

    return None


def _clean_json_from_text(text: str, viz_data: dict[str, Any]) -> str:
    """Remove the embedded JSON block from the agent's text to get clean insight."""
    # Try to find and remove the JSON block
    for marker in ('"visualization"', '"visualizacion"'):
        idx = text.find(marker)
        if idx == -1:
            continue
        start = text.rfind("{", 0, idx)
        if start == -1:
            continue
        json_str = _extract_balanced_json(text, start)
        if json_str:
            # Also remove surrounding markdown fences if present
            fence_start = text.rfind("```", 0, start)
            fence_end = text.find("```", start + len(json_str))
            if fence_start != -1 and fence_end != -1 and (start - fence_start) < 20:
                text = text[:fence_start] + text[fence_end + 3:]
            else:
                text = text[:start] + text[start + len(json_str):]
            return text.strip()

    return text.strip()
