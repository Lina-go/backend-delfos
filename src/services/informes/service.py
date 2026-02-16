"""Informe persistence service."""

import contextlib
import json
import logging
import uuid
from typing import Any

from fastapi import HTTPException

from src.api.models import (
    CreateInformeRequest,
    InformeDetail,
    InformeGraph,
    InformeSummary,
)
from src.config.settings import Settings
from src.infrastructure.database.connection import execute_insert, execute_query
from src.infrastructure.database.helpers import audit_log, check_db_result
from src.orchestrator.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)


class InformeService:
    """Handles informe CRUD operations."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def list_informes(
        self,
        owner: str | None,
        offset: int,
        limit: int,
    ) -> list[InformeSummary]:
        """List all informes with graph count."""
        sql = """
        SELECT p.id, p.title, p.description, p.owner, p.createdAt AS created_at,
               COUNT(pi.id) AS graph_count
        FROM dbo.Projects p
        LEFT JOIN dbo.ProjectItems pi ON pi.projectId = p.id AND pi.graph_id IS NOT NULL
        {where}
        GROUP BY p.id, p.title, p.description, p.owner, p.createdAt
        ORDER BY p.createdAt DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        if owner:
            rows = await execute_query(
                self.settings, sql.format(where="WHERE p.owner = ?"), (owner, offset, limit)
            )
        else:
            rows = await execute_query(self.settings, sql.format(where=""), (offset, limit))

        return [InformeSummary.from_db_row(r) for r in rows]

    async def create_informe(self, request: CreateInformeRequest) -> InformeSummary:
        """Create a new informe."""
        new_id = str(uuid.uuid4())
        result = await execute_insert(
            self.settings,
            "INSERT INTO dbo.Projects (id, title, description, owner, createdAt) VALUES (?, ?, ?, ?, GETDATE())",
            (new_id, request.title, request.description, request.owner),
        )
        check_db_result(result, "create informe")
        audit_log("CREATE", "informe", new_id)

        return InformeSummary(
            id=new_id,
            title=request.title,
            description=request.description,
            owner=request.owner,
            graph_count=0,
        )

    async def get_informe(self, informe_id: str) -> InformeDetail:
        """Get informe detail with all its graphs."""
        header_rows = await execute_query(
            self.settings,
            "SELECT id, title, description, owner, createdAt AS created_at FROM dbo.Projects WHERE id = ?",
            (informe_id,),
        )
        if not header_rows:
            raise HTTPException(status_code=404, detail="Informe not found")
        h = header_rows[0]

        graphs_data = await execute_query(
            self.settings,
            """
            SELECT pi.id AS item_id, g.id AS graph_id, g.type, g.content, g.title, g.query, g.createdAt AS created_at
            FROM dbo.ProjectItems pi
            INNER JOIN dbo.Graphs g ON pi.graph_id = g.id
            WHERE pi.projectId = ?
            ORDER BY pi.createdAt ASC
            """,
            (informe_id,),
        )

        graphs = [InformeGraph.from_db_row(row) for row in graphs_data]

        return InformeDetail(
            id=str(h["id"]),
            title=str(h["title"]),
            description=h.get("description"),
            owner=h.get("owner"),
            created_at=h.get("created_at"),
            graphs=graphs,
        )

    async def delete_informe(self, informe_id: str) -> None:
        """Delete an informe and its graph associations (not the graphs themselves)."""
        items_result = await execute_insert(
            self.settings, "DELETE FROM dbo.ProjectItems WHERE projectId = ?", (informe_id,)
        )
        check_db_result(items_result, "delete informe items")
        result = await execute_insert(
            self.settings, "DELETE FROM dbo.Projects WHERE id = ?", (informe_id,)
        )
        check_db_result(result, "delete informe")
        audit_log("DELETE", "informe", informe_id)

    async def add_graphs(self, informe_id: str, graph_ids: list[str]) -> dict[str, Any]:
        """Add one or more graphs to an informe."""
        if not graph_ids:
            raise HTTPException(status_code=400, detail="No graph IDs provided")

        # Verify informe exists
        if not await execute_query(
            self.settings, "SELECT id FROM dbo.Projects WHERE id = ?", (informe_id,)
        ):
            raise HTTPException(status_code=404, detail="Informe not found")

        # Find which graphs exist and which are already linked
        ph = ", ".join(["?" for _ in graph_ids])
        existing = await execute_query(
            self.settings,
            f"SELECT id, title FROM dbo.Graphs WHERE id IN ({ph})",
            tuple(graph_ids),
        )
        existing_map = {str(g["id"]): str(g["title"]) for g in existing}

        already = await execute_query(
            self.settings,
            f"SELECT graph_id FROM dbo.ProjectItems WHERE projectId = ? AND graph_id IN ({ph})",
            (informe_id, *graph_ids),
        )
        already_ids = {str(r["graph_id"]) for r in already}

        added: list[str] = []
        skipped: list[str] = []
        not_found: list[str] = []

        for gid in graph_ids:
            if gid not in existing_map:
                not_found.append(gid)
            elif gid in already_ids:
                skipped.append(gid)
            else:
                result = await execute_insert(
                    self.settings,
                    "INSERT INTO dbo.ProjectItems (id, projectId, type, content, title, graph_id, createdAt) VALUES (?, ?, 'graph', '', ?, ?, GETDATE())",
                    (str(uuid.uuid4()), informe_id, existing_map[gid], gid),
                )
                if result.get("success"):
                    added.append(gid)

        return {
            "status": "success",
            "added": added,
            "skipped_duplicates": skipped,
            "not_found": not_found,
        }

    async def remove_graph(self, informe_id: str, item_id: str) -> None:
        """Remove a graph from an informe (does NOT delete the graph itself)."""
        result = await execute_insert(
            self.settings,
            "DELETE FROM dbo.ProjectItems WHERE id = ? AND projectId = ?",
            (item_id, informe_id),
        )
        check_db_result(result, "remove graph from informe")
        audit_log("DELETE", "informe_graph", item_id)

    async def refresh_informe(self, informe_id: str) -> dict[str, Any]:
        """Refresh all graphs in an informe by re-executing their stored queries."""
        items = await execute_query(
            self.settings,
            "SELECT graph_id FROM dbo.ProjectItems WHERE projectId = ? AND graph_id IS NOT NULL",
            (informe_id,),
        )
        if not items:
            raise HTTPException(status_code=404, detail="Informe has no graphs")

        refreshed: list[str] = []
        failed: list[dict[str, str]] = []
        skipped: list[dict[str, str]] = []
        orchestrator = PipelineOrchestrator(self.settings)

        try:
            for item in items:
                gid = str(item["graph_id"])
                graph_rows = await execute_query(
                    self.settings,
                    "SELECT type, title, query, user_id FROM dbo.Graphs WHERE id = ?",
                    (gid,),
                )
                if not graph_rows:
                    failed.append({"id": gid, "error": "Graph not found"})
                    continue

                g = graph_rows[0]
                if not g.get("query"):
                    skipped.append({"id": gid, "reason": "No stored query"})
                    continue

                try:
                    result = await orchestrator.refresh_graph(
                        sql=g["query"],
                        chart_type=g.get("type", "bar"),
                        title=g.get("title", "Visualización"),
                        user_id=g.get("user_id", "system"),
                    )
                    if result.get("error"):
                        failed.append({"id": gid, "error": result["error"]})
                        continue

                    new_content = json.dumps(
                        {
                            "data_points": result.get("data_points", []),
                            "metric_name": result.get("metric_name"),
                        },
                        ensure_ascii=False,
                    )
                    update_result = await execute_insert(
                        self.settings,
                        "UPDATE dbo.Graphs SET content = ?, createdAt = GETDATE() WHERE id = ?",
                        (new_content, gid),
                    )
                    if update_result.get("success"):
                        refreshed.append(gid)
                    else:
                        logger.error(
                            "Failed to update graph %s: %s", gid, update_result.get("error")
                        )
                        failed.append({"id": gid, "error": "DB update failed"})
                except Exception as e:
                    logger.error("Error refreshing graph %s: %s", gid, e)
                    failed.append({"id": gid, "error": "Refresh failed"})
        finally:
            with contextlib.suppress(Exception):
                await orchestrator.close()

        return {
            "status": "success",
            "informe_id": informe_id,
            "refreshed": refreshed,
            "failed": failed,
            "skipped": skipped,
        }

    async def delete_bulk(self, informe_ids: list[str]) -> int:
        """Delete multiple informes and their associations. Returns count deleted."""
        if not informe_ids:
            raise HTTPException(status_code=400, detail="No informe IDs provided")

        placeholders = ", ".join(["?" for _ in informe_ids])
        items_result = await execute_insert(
            self.settings,
            f"DELETE FROM dbo.ProjectItems WHERE projectId IN ({placeholders})",
            tuple(informe_ids),
        )
        check_db_result(items_result, "delete informe items")
        result = await execute_insert(
            self.settings,
            f"DELETE FROM dbo.Projects WHERE id IN ({placeholders})",
            tuple(informe_ids),
        )
        check_db_result(result, "delete informes")
        for iid in informe_ids:
            audit_log("DELETE", "informe", iid)

        return len(informe_ids)
