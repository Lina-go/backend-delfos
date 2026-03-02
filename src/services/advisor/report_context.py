"""Build and cache report context for the advisor agent."""

import asyncio
import logging
import time
from typing import Any

from src.config.settings import Settings
from src.services.informes import InformeService
from src.utils.graph_data import fetch_graph_data, parse_graph_content, truncate_data_points

logger = logging.getLogger(__name__)

# Total data_points budget across ALL graphs to stay within the LLM token window.
# Divided equally among graphs so more graphs = fewer rows each.
_TOTAL_DATA_POINTS_BUDGET = 100

# ---------------------------------------------------------------------------
# Report context cache — keyed by informe_id, shared across users.
# ---------------------------------------------------------------------------
_CONTEXT_CACHE_TTL = 300  # 5 minutes
_context_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def invalidate_context_cache(informe_id: str) -> None:
    """Remove a cached report context so the next request rebuilds it."""
    _context_cache.pop(informe_id, None)


async def _build_report_context(informe_id: str, settings: Settings) -> dict[str, Any]:
    """Build report_context dict from DB and warehouse, fetching graph data in parallel."""
    svc = InformeService(settings)
    informe = await svc.get_informe(informe_id)

    # --- Phase 0: deduplicate graphs by graph_id (same graph can appear in multiple tabs) ---
    seen_graph_ids: set[str] = set()
    unique_graphs = []
    for g in informe.graphs:
        if g.graph_id not in seen_graph_ids:
            seen_graph_ids.add(g.graph_id)
            unique_graphs.append(g)

    n_graphs = max(len(unique_graphs), 1)
    per_graph_limit = _TOTAL_DATA_POINTS_BUDGET // n_graphs

    # --- Phase 1: parse content & identify graphs that need warehouse fetch ---
    parsed_list: list[tuple[Any, bool]] = []  # (parsed_content, has_data_points)
    fetch_indices: list[int] = []
    fetch_queries: list[str] = []

    for i, g in enumerate(unique_graphs):
        parsed = parse_graph_content(g.content)
        has_dp = isinstance(parsed, dict) and "data_points" in parsed

        if not has_dp and g.query:
            fetch_indices.append(i)
            fetch_queries.append(g.query)
        elif has_dp and isinstance(parsed.get("data_points"), list):
            parsed["data_points"] = truncate_data_points(parsed["data_points"], per_graph_limit)

        parsed_list.append((parsed, has_dp))

    # --- Phase 2: fetch missing data_points in parallel ---
    if fetch_queries:
        results = await asyncio.gather(
            *[
                fetch_graph_data(settings, q, unique_graphs[idx].title)
                for idx, q in zip(fetch_indices, fetch_queries)
            ],
            return_exceptions=True,
        )
        for idx, result in zip(fetch_indices, results):
            if isinstance(result, list) and result:
                parsed_list[idx] = (
                    {"data_points": truncate_data_points(result, per_graph_limit)},
                    True,
                )

    # --- Phase 3: assemble final context ---
    graphs = []
    for i, g in enumerate(unique_graphs):
        parsed, has_dp = parsed_list[i]
        graphs.append({
            "graph_id": g.graph_id,
            "title": g.title,
            "type": g.type,
            "label_name": g.label_name,
            "content": parsed,
            "has_data_points": has_dp,
        })

    logger.info(
        "[ADVISOR CONTEXT] informe=%s total_raw=%d unique=%d titles=%s",
        informe_id,
        len(informe.graphs),
        len(graphs),
        [g["title"] for g in graphs],
    )

    return {
        "informe_id": informe.id,
        "title": informe.title,
        "description": informe.description,
        "owner": informe.owner,
        "total_graphs": len(graphs),
        "labels": [
            {"id": lbl.id, "name": lbl.name, "chart_count": lbl.chart_count}
            for lbl in informe.labels
        ],
        "graphs": graphs,
    }


async def get_report_context(informe_id: str, settings: Settings) -> dict[str, Any]:
    """Return report context, using an in-memory cache with TTL."""
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
