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
    InformeLabel,
    InformeSummary,
    LabelSuggestion,
    SuggestLabelsResponse,
    _parse_json_field,
)
from src.config.settings import Settings
from src.infrastructure.database.connection import execute_insert, execute_query
from src.infrastructure.database.helpers import audit_log, check_db_result
from src.orchestrator.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)

_LABEL_WITH_COUNT_SQL = """
    SELECT l.id, l.informe_id, l.name, l.createdAt AS created_at,
           COUNT(pi.id) AS chart_count
    FROM dbo.InformeLabels l
    LEFT JOIN dbo.ProjectItems pi ON pi.label_id = l.id AND pi.graph_id IS NOT NULL
    {where}
    GROUP BY l.id, l.informe_id, l.name, l.createdAt
"""


class InformeService:
    """Handles informe CRUD operations."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    # -----------------------------------------------------------------------
    # Private validation helpers
    # -----------------------------------------------------------------------

    async def _verify_informe_exists(self, informe_id: str) -> None:
        """Raise 404 if informe does not exist."""
        if not await execute_query(
            self.settings, "SELECT id FROM dbo.Projects WHERE id = ?", (informe_id,)
        ):
            raise HTTPException(status_code=404, detail="Informe not found")

    async def _verify_label_exists(self, informe_id: str, label_id: str) -> None:
        """Raise 404 if label does not belong to the given informe."""
        rows = await execute_query(
            self.settings,
            "SELECT id FROM dbo.InformeLabels WHERE id = ? AND informe_id = ?",
            (label_id, informe_id),
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Label not found in this informe")

    async def _check_label_name_unique(
        self, informe_id: str, name: str, exclude_label_id: str | None = None
    ) -> None:
        """Raise 400 if a label with the given name already exists in the informe."""
        if exclude_label_id:
            existing = await execute_query(
                self.settings,
                "SELECT id FROM dbo.InformeLabels WHERE informe_id = ? AND name = ? AND id != ?",
                (informe_id, name, exclude_label_id),
            )
        else:
            existing = await execute_query(
                self.settings,
                "SELECT id FROM dbo.InformeLabels WHERE informe_id = ? AND name = ?",
                (informe_id, name),
            )
        if existing:
            raise HTTPException(
                status_code=400, detail=f"Label '{name}' already exists in this informe"
            )

    # -----------------------------------------------------------------------
    # Informe CRUD
    # -----------------------------------------------------------------------

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
        """Get informe detail with all its graphs and labels."""
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
            SELECT pi.id AS item_id, g.id AS graph_id, g.type, g.content, g.title, g.query,
                   g.createdAt AS created_at, pi.label_id, l.name AS label_name
            FROM dbo.ProjectItems pi
            INNER JOIN dbo.Graphs g ON pi.graph_id = g.id
            LEFT JOIN dbo.InformeLabels l ON pi.label_id = l.id
            WHERE pi.projectId = ?
            ORDER BY l.name ASC, pi.createdAt ASC
            """,
            (informe_id,),
        )

        graphs = [InformeGraph.from_db_row(row) for row in graphs_data]
        labels = await self.list_labels(informe_id)

        return InformeDetail(
            id=str(h["id"]),
            title=str(h["title"]),
            description=h.get("description"),
            owner=h.get("owner"),
            created_at=h.get("created_at"),
            graphs=graphs,
            labels=labels,
        )

    async def delete_informe(self, informe_id: str) -> None:
        """Delete an informe, its labels, and graph associations (not the graphs themselves)."""
        items_result = await execute_insert(
            self.settings, "DELETE FROM dbo.ProjectItems WHERE projectId = ?", (informe_id,)
        )
        check_db_result(items_result, "delete informe items")
        labels_result = await execute_insert(
            self.settings, "DELETE FROM dbo.InformeLabels WHERE informe_id = ?", (informe_id,)
        )
        check_db_result(labels_result, "delete informe labels")
        result = await execute_insert(
            self.settings, "DELETE FROM dbo.Projects WHERE id = ?", (informe_id,)
        )
        check_db_result(result, "delete informe")
        audit_log("DELETE", "informe", informe_id)

    async def add_graphs(
        self, informe_id: str, graph_ids: list[str], label_id: str | None = None
    ) -> dict[str, Any]:
        """Add one or more graphs to an informe, optionally under a label."""
        if not graph_ids:
            raise HTTPException(status_code=400, detail="No graph IDs provided")

        await self._verify_informe_exists(informe_id)
        if label_id:
            await self._verify_label_exists(informe_id, label_id)

        placeholders = ", ".join(["?" for _ in graph_ids])
        existing = await execute_query(
            self.settings,
            f"SELECT id, title FROM dbo.Graphs WHERE id IN ({placeholders})",
            tuple(graph_ids),
        )
        existing_map = {str(g["id"]): str(g["title"]) for g in existing}

        already = await execute_query(
            self.settings,
            f"SELECT graph_id FROM dbo.ProjectItems WHERE projectId = ? AND graph_id IN ({placeholders})",
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
                    "INSERT INTO dbo.ProjectItems (id, projectId, type, content, title, graph_id, label_id, createdAt) VALUES (?, ?, 'graph', '', ?, ?, ?, GETDATE())",
                    (str(uuid.uuid4()), informe_id, existing_map[gid], gid, label_id),
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
        """Delete multiple informes, their labels, and associations. Returns count deleted."""
        if not informe_ids:
            raise HTTPException(status_code=400, detail="No informe IDs provided")

        placeholders = ", ".join(["?" for _ in informe_ids])
        items_result = await execute_insert(
            self.settings,
            f"DELETE FROM dbo.ProjectItems WHERE projectId IN ({placeholders})",
            tuple(informe_ids),
        )
        check_db_result(items_result, "delete informe items")
        labels_result = await execute_insert(
            self.settings,
            f"DELETE FROM dbo.InformeLabels WHERE informe_id IN ({placeholders})",
            tuple(informe_ids),
        )
        check_db_result(labels_result, "delete informe labels")
        result = await execute_insert(
            self.settings,
            f"DELETE FROM dbo.Projects WHERE id IN ({placeholders})",
            tuple(informe_ids),
        )
        check_db_result(result, "delete informes")
        for iid in informe_ids:
            audit_log("DELETE", "informe", iid)

        return len(informe_ids)

    # -----------------------------------------------------------------------
    # Label CRUD
    # -----------------------------------------------------------------------

    async def list_labels(self, informe_id: str) -> list[InformeLabel]:
        """List all labels for an informe with chart counts."""
        sql = _LABEL_WITH_COUNT_SQL.format(where="WHERE l.informe_id = ?") + "\n    ORDER BY l.name ASC"
        rows = await execute_query(self.settings, sql, (informe_id,))
        return [InformeLabel.from_db_row(r) for r in rows]

    async def create_label(self, informe_id: str, name: str) -> InformeLabel:
        """Create a new label within an informe."""
        await self._verify_informe_exists(informe_id)
        await self._check_label_name_unique(informe_id, name)

        label_id = str(uuid.uuid4())
        result = await execute_insert(
            self.settings,
            "INSERT INTO dbo.InformeLabels (id, informe_id, name, createdAt) VALUES (?, ?, ?, GETDATE())",
            (label_id, informe_id, name),
        )
        check_db_result(result, "create label")
        audit_log("CREATE", "informe_label", label_id)

        return InformeLabel(id=label_id, informe_id=informe_id, name=name, chart_count=0)

    async def update_label(self, informe_id: str, label_id: str, name: str) -> InformeLabel:
        """Rename a label within an informe."""
        await self._verify_label_exists(informe_id, label_id)
        await self._check_label_name_unique(informe_id, name, exclude_label_id=label_id)

        result = await execute_insert(
            self.settings,
            "UPDATE dbo.InformeLabels SET name = ? WHERE id = ?",
            (name, label_id),
        )
        check_db_result(result, "update label")
        audit_log("UPDATE", "informe_label", label_id)

        sql = _LABEL_WITH_COUNT_SQL.format(where="WHERE l.id = ?")
        updated = await execute_query(self.settings, sql, (label_id,))
        return InformeLabel.from_db_row(updated[0])

    async def delete_label(self, informe_id: str, label_id: str) -> None:
        """Delete a label. Only allowed if no charts are assigned to it."""
        await self._verify_label_exists(informe_id, label_id)

        charts = await execute_query(
            self.settings,
            "SELECT id FROM dbo.ProjectItems WHERE label_id = ? AND graph_id IS NOT NULL",
            (label_id,),
        )
        if charts:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot delete label: {len(charts)} chart(s) still assigned. Move charts to another label first.",
            )

        result = await execute_insert(
            self.settings,
            "DELETE FROM dbo.InformeLabels WHERE id = ?",
            (label_id,),
        )
        check_db_result(result, "delete label")
        audit_log("DELETE", "informe_label", label_id)

    async def update_graph_label(
        self, informe_id: str, item_id: str, label_id: str | None
    ) -> None:
        """Change a chart's label (move to different tab) or remove label assignment."""
        item_rows = await execute_query(
            self.settings,
            "SELECT id FROM dbo.ProjectItems WHERE id = ? AND projectId = ? AND graph_id IS NOT NULL",
            (item_id, informe_id),
        )
        if not item_rows:
            raise HTTPException(status_code=404, detail="Chart not found in this informe")

        if label_id:
            await self._verify_label_exists(informe_id, label_id)

        result = await execute_insert(
            self.settings,
            "UPDATE dbo.ProjectItems SET label_id = ? WHERE id = ?",
            (label_id, item_id),
        )
        check_db_result(result, "update chart label")
        audit_log("UPDATE", "informe_graph_label", item_id)

    async def suggest_labels(self, graph_ids: list[str]) -> list[LabelSuggestion]:
        """Suggest label groupings for selected graphs using an LLM."""
        from src.config.prompts import build_suggest_labels_system_prompt
        from src.infrastructure.llm.executor import run_agent_with_format
        from src.infrastructure.llm.factory import azure_agent_client, get_shared_credential

        placeholders = ", ".join(["?" for _ in graph_ids])
        rows = await execute_query(
            self.settings,
            f"SELECT id, type, content, title FROM dbo.Graphs WHERE id IN ({placeholders})",
            tuple(graph_ids),
        )
        if not rows:
            raise HTTPException(status_code=404, detail="No graphs found for the provided IDs")

        lines: list[str] = []
        for row in rows:
            graph_id = str(row["id"])
            graph_type = str(row.get("type", "unknown")).upper()
            title = str(row.get("title", ""))
            metric_name = _parse_json_field(row.get("content")).get("metric_name", "")
            parts = [f"[{graph_id}] {graph_type} | {title}"]
            if metric_name:
                parts.append(f"metric: {metric_name}")
            lines.append(" | ".join(parts))

        graphs_summary = "\n".join(lines)
        user_message = f"Organize these {len(lines)} charts into labels:\n\n{graphs_summary}"

        try:
            system_prompt = build_suggest_labels_system_prompt()
            model = self.settings.suggest_labels_agent_model
            credential = get_shared_credential()

            async with azure_agent_client(self.settings, model, credential, max_iterations=2) as client:
                agent = client.create_agent(
                    name="SuggestLabels",
                    instructions=system_prompt,
                    max_tokens=self.settings.suggest_labels_max_tokens,
                    temperature=self.settings.suggest_labels_temperature,
                    response_format=SuggestLabelsResponse,
                )
                result = await run_agent_with_format(
                    agent, user_message, response_format=SuggestLabelsResponse
                )

            if isinstance(result, SuggestLabelsResponse):
                return result.suggestions

            from src.utils.json_parser import JSONParser

            json_data = JSONParser.extract_json(str(result))
            if json_data:
                parsed = SuggestLabelsResponse(**json_data)
                return parsed.suggestions

            raise ValueError("LLM returned unparseable response")

        except HTTPException:
            raise
        except Exception as e:
            logger.error("suggest_labels LLM error: %s", e, exc_info=True)
            raise HTTPException(
                status_code=500, detail="Failed to generate label suggestions"
            ) from e
