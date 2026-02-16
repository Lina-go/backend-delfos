"""Graph persistence service."""

import json
import logging
import uuid

from fastapi import HTTPException

from src.api.models import Graph, SaveGraphRequest
from src.config.settings import Settings
from src.infrastructure.database.connection import execute_insert, execute_query
from src.infrastructure.database.helpers import audit_log, check_db_result
from src.orchestrator.pipeline import PipelineOrchestrator

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
            sql = "SELECT id, type, content, title, query, createdAt as created_at, metadata, user_id FROM dbo.Graphs WHERE user_id = ? ORDER BY createdAt DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
            graphs_data = await execute_query(self.settings, sql, (user_id, offset, limit))
        else:
            sql = "SELECT id, type, content, title, query, createdAt as created_at, metadata, user_id FROM dbo.Graphs ORDER BY createdAt DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
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
        # 1. Fetch graph from DB
        graphs_data = await execute_query(
            self.settings,
            "SELECT type, content, title, query, user_id FROM dbo.Graphs WHERE id = ?",
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

        # 3. Update graph in DB with new content (data_points JSON)
        new_content = json.dumps(
            {"data_points": result.get("data_points", []), "metric_name": result.get("metric_name")},
            ensure_ascii=False,
        )
        update_result = await execute_insert(
            self.settings,
            "UPDATE dbo.Graphs SET content = ?, createdAt = GETDATE() WHERE id = ?",
            (new_content, graph_id),
        )
        if not update_result.get("success") or update_result.get("error"):
            logger.error("Failed to update graph %s: %s", graph_id, update_result.get("error"))
            raise HTTPException(status_code=500, detail="Failed to update graph")

        # 4. Re-fetch updated graph and return full Graph object
        updated_rows = await execute_query(
            self.settings,
            "SELECT id, type, content, title, query, createdAt as created_at, metadata, user_id FROM dbo.Graphs WHERE id = ?",
            (graph_id,),
        )
        if not updated_rows:
            raise HTTPException(status_code=500, detail="Graph not found after update")

        return Graph.from_db_row(updated_rows[0])
