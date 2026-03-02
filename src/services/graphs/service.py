"""Graph persistence service."""

import asyncio
import json
import logging
import uuid
from typing import Any

from fastapi import HTTPException

from src.api.models import Graph, GraphBulletItem, SaveGraphRequest
from src.config.settings import Settings
from src.infrastructure.database.connection import execute_insert, execute_query
from src.infrastructure.database.helpers import audit_log, check_db_result
from src.orchestrator.pipeline import PipelineOrchestrator
from src.utils.graph_data import fetch_graph_data, parse_graph_content, truncate_data_points

logger = logging.getLogger(__name__)


class GraphService:
    """Handles graph CRUD operations."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def list_graphs(
        self,
        user_id: str | None,
        offset: int,
        limit: int,
    ) -> list[Graph]:
        """List saved graphs, optionally filtered by user_id."""
        if user_id:
            sql = "SELECT id, type, content, title, query, createdAt as created_at, metadata, user_id, bullet FROM dbo.Graphs WHERE user_id = ? ORDER BY createdAt DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
            graphs_data = await execute_query(self.settings, sql, (user_id, offset, limit))
        else:
            sql = "SELECT id, type, content, title, query, createdAt as created_at, metadata, user_id, bullet FROM dbo.Graphs ORDER BY createdAt DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
            graphs_data = await execute_query(self.settings, sql, (offset, limit))

        return [Graph.from_db_row(g) for g in graphs_data]

    async def save_graph(self, request: SaveGraphRequest) -> str:
        """Save a graph from chat. Returns the new graph_id."""
        graph_id = str(uuid.uuid4())
        metadata_str = json.dumps(request.metadata) if request.metadata else None

        sql = """
        INSERT INTO dbo.Graphs (id, type, content, title, query, metadata, user_id, createdAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        result = await execute_insert(
            self.settings,
            sql,
            (
                graph_id,
                request.type,
                request.content,
                request.title,
                request.query,
                metadata_str,
                request.user_id,
            ),
        )
        check_db_result(result, "save graph")
        audit_log("CREATE", "graph", graph_id)

        return graph_id

    async def delete_graph(self, graph_id: str) -> None:
        """Delete a single graph."""
        result = await execute_insert(
            self.settings, "DELETE FROM dbo.Graphs WHERE id = ?", (graph_id,)
        )
        check_db_result(result, "delete graph")
        audit_log("DELETE", "graph", graph_id)

    async def delete_bulk(self, graph_ids: list[str]) -> int:
        """Delete multiple graphs at once. Returns the count deleted."""
        if not graph_ids:
            raise HTTPException(status_code=400, detail="No graph IDs provided")

        placeholders = ", ".join(["?" for _ in graph_ids])
        result = await execute_insert(
            self.settings,
            f"DELETE FROM dbo.Graphs WHERE id IN ({placeholders})",
            tuple(graph_ids),
        )
        check_db_result(result, "delete graphs")
        for gid in graph_ids:
            audit_log("DELETE", "graph", gid)

        return len(graph_ids)

    async def refresh_graph(self, graph_id: str) -> Graph:
        """Re-execute a graph's stored query and regenerate its visualization."""
        graphs_data = await execute_query(
            self.settings,
            "SELECT type, content, title, query, metadata, user_id FROM dbo.Graphs WHERE id = ?",
            (graph_id,),
        )
        if not graphs_data:
            raise HTTPException(status_code=404, detail="Graph not found")

        graph = graphs_data[0]
        sql_query = graph.get("query")
        if not sql_query:
            raise HTTPException(status_code=400, detail="Graph has no stored query to refresh")

        chart_type = graph.get("type", "bar")
        title = graph.get("title", "Visualización")
        user_id = graph.get("user_id", "system")

        # 2. Re-execute query and regenerate viz
        async with PipelineOrchestrator(self.settings) as orchestrator:
            try:
                result = await orchestrator.refresh_graph(
                    sql=sql_query,
                    chart_type=chart_type,
                    title=title,
                    user_id=user_id,
                )
            except Exception as e:
                logger.error("Error refreshing graph %s: %s", graph_id, e, exc_info=True)
                raise HTTPException(status_code=500, detail="Failed to refresh graph") from e

        if result.get("error"):
            logger.error("Graph refresh returned error for %s: %s", graph_id, result["error"])
            raise HTTPException(status_code=500, detail="Failed to refresh graph")

        # 3. Update graph in DB with new content (data_points JSON + indicators)
        data_points = result.get("data_points", [])
        content_dict: dict[str, Any] = {
            "data_points": data_points,
            "metric_name": result.get("metric_name"),
        }

        # Recompute indicators from saved indicator_specs + new data_points
        old_content = graph.get("content", "")
        try:
            old_parsed = json.loads(old_content) if old_content else {}
        except (json.JSONDecodeError, TypeError):
            old_parsed = {}

        # Also check metadata (frontend saves specs there)
        raw_meta = graph.get("metadata", "")
        try:
            meta_parsed = json.loads(raw_meta) if raw_meta else {}
        except (json.JSONDecodeError, TypeError):
            meta_parsed = {}

        saved_specs = (
            old_parsed.get("indicator_specs")
            or meta_parsed.get("indicator_specs")
            or []
        )
        if saved_specs and data_points:
            from src.services.chat_v2.indicators import compute_series_stats, resolve_indicators
            from src.services.chat_v2.models import IndicatorSpec

            try:
                specs = [IndicatorSpec(**s) for s in saved_specs]
                series_stats = compute_series_stats(data_points)
                indicators = resolve_indicators(series_stats, specs)
                content_dict["indicators"] = indicators
                content_dict["indicator_specs"] = saved_specs
            except Exception as e:
                logger.warning("Failed to recompute indicators for graph %s: %s", graph_id, e)
                # Preserve old indicators/specs as fallback
                if old_parsed.get("indicators"):
                    content_dict["indicators"] = old_parsed["indicators"]
                if saved_specs:
                    content_dict["indicator_specs"] = saved_specs

        new_content = json.dumps(content_dict, ensure_ascii=False)
        update_result = await execute_insert(
            self.settings,
            "UPDATE dbo.Graphs SET content = ?, bullet = NULL, createdAt = GETDATE() WHERE id = ?",
            (new_content, graph_id),
        )
        if not update_result.get("success") or update_result.get("error"):
            logger.error("Failed to update graph %s: %s", graph_id, update_result.get("error"))
            raise HTTPException(status_code=500, detail="Failed to update graph")

        # 4. Re-fetch updated graph and return full Graph object
        updated_rows = await execute_query(
            self.settings,
            "SELECT id, type, content, title, query, createdAt as created_at, metadata, user_id, bullet FROM dbo.Graphs WHERE id = ?",
            (graph_id,),
        )
        if not updated_rows:
            raise HTTPException(status_code=500, detail="Graph not found after update")

        return Graph.from_db_row(updated_rows[0])

    # ------------------------------------------------------------------
    # Bullet generation
    # ------------------------------------------------------------------

    async def generate_bullets(self, informe_id: str) -> list[GraphBulletItem]:
        """Generate a bullet point for each graph in an informe.

        Bullets are cached in ``dbo.Graphs.bullet``.  Only graphs with
        ``bullet IS NULL`` trigger an LLM call; the rest are returned from DB.
        """
        # 1. Fetch graphs for this informe (deduplicated by graph_id)
        rows = await execute_query(
            self.settings,
            """
            SELECT DISTINCT g.id AS graph_id, g.title, g.content, g.query, g.bullet
            FROM dbo.ProjectItems pi
            INNER JOIN dbo.Graphs g ON pi.graph_id = g.id
            WHERE pi.projectId = ?
            """,
            (informe_id,),
        )
        if not rows:
            return []

        # 2. Separate cached vs needs-generation
        cached: list[GraphBulletItem] = []
        to_generate: list[dict[str, Any]] = []

        for row in rows:
            if row.get("bullet"):
                cached.append(GraphBulletItem(
                    graph_id=str(row["graph_id"]),
                    title=str(row.get("title", "")),
                    bullet=row["bullet"],
                ))
            else:
                to_generate.append(row)

        if not to_generate:
            return cached

        # 3. Resolve data_points for graphs that need generation
        _PER_GRAPH_LIMIT = 30

        async def _resolve_data(row: dict[str, Any]) -> str:
            """Return a JSON string with the graph's data for the LLM."""
            content = parse_graph_content(row.get("content", ""))
            if isinstance(content, dict) and "data_points" in content:
                dp = truncate_data_points(content["data_points"], _PER_GRAPH_LIMIT)
                return json.dumps(dp, ensure_ascii=False)

            # Fall back to warehouse fetch
            query = row.get("query")
            if query:
                wh_data = await fetch_graph_data(
                    self.settings, query, row.get("title", "")
                )
                if wh_data:
                    dp = truncate_data_points(wh_data, _PER_GRAPH_LIMIT)
                    return json.dumps(dp, ensure_ascii=False)

            return "Sin datos disponibles."

        data_strings = await asyncio.gather(
            *[_resolve_data(r) for r in to_generate]
        )

        # 4. Call LLM in parallel for each graph
        from src.config.prompts.bullets import build_graph_bullet_system_prompt
        from src.orchestrator.handlers._llm_helper import run_handler_agent

        system_prompt = build_graph_bullet_system_prompt()

        async def _generate_one(row: dict[str, Any], data_str: str) -> GraphBulletItem:
            title = str(row.get("title", ""))
            user_msg = (
                f"Titulo del grafico: {title}\n"
                f"Datos:\n{data_str}"
            )
            bullet_text = await run_handler_agent(
                self.settings,
                name="GraphBulletAgent",
                instructions=system_prompt,
                message=user_msg,
                model=self.settings.graph_bullets_model,
                max_tokens=self.settings.graph_bullets_max_tokens,
                temperature=self.settings.graph_bullets_temperature,
            )
            bullet_text = bullet_text.strip().strip('"').strip("'")
            return GraphBulletItem(
                graph_id=str(row["graph_id"]),
                title=title,
                bullet=bullet_text,
            )

        generated = await asyncio.gather(
            *[
                _generate_one(row, data_str)
                for row, data_str in zip(to_generate, data_strings)
            ],
            return_exceptions=True,
        )

        # 5. Persist and collect results
        new_bullets: list[GraphBulletItem] = []
        for result in generated:
            if isinstance(result, Exception):
                logger.error("Bullet generation failed: %s", result, exc_info=result)
                continue
            # Persist to DB
            try:
                await execute_insert(
                    self.settings,
                    "UPDATE dbo.Graphs SET bullet = ? WHERE id = ?",
                    (result.bullet, result.graph_id),
                )
            except Exception as e:
                logger.error(
                    "Failed to persist bullet for graph %s: %s",
                    result.graph_id, e,
                )
            new_bullets.append(result)

        return cached + new_bullets
