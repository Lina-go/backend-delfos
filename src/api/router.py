"""API routes."""

import json
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlparse, urlunparse

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from src.api.dependencies import get_settings_dependency
from src.api.models import (
    AddProjectItemRequest,
    ChatRequest,
    ChatResponse,
    CreateProjectRequest,
    DeleteGraphsRequest,
    DeleteInformesRequest,
    Graph,
    HealthResponse,
    Project,
    SaveGraphRequest,
    InformeSummary,
    InformeGraph,
    InformeDetail,
    CreateInformeRequest,
    AddGraphsToInformeRequest,
)
from src.config.settings import Settings
from src.infrastructure.cache.semantic_cache import SemanticCache
from src.infrastructure.database.connection import execute_insert, execute_query
from src.infrastructure.storage.blob_client import BlobStorageClient
from src.orchestrator.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
#  HELPERS
# ==========================================


def _clean_blob_url(url: str) -> str:
    """Strip SAS token from a blob storage URL."""
    if not url or "blob.core.windows.net" not in url:
        return url
    try:
        parsed = urlparse(url)
        return urlunparse(parsed._replace(query=""))
    except Exception as e:
        logger.warning(f"Could not clean URL: {e}")
        return url


async def _sign_blob_url(url: str, storage_client: BlobStorageClient, container_name: str) -> str:
    """Generate a fresh SAS-signed URL for a blob."""
    if not url or "blob.core.windows.net" not in url:
        return url
    try:
        parsed = urlparse(url.split("?")[0])
        path_parts = parsed.path.strip("/").split("/", 1)
        if len(path_parts) > 1:
            return await storage_client.get_blob_sas_url(
                container_name=container_name, blob_name=path_parts[1]
            )
        logger.warning(f"Could not extract blob name from: {url}")
    except Exception as e:
        logger.warning(f"Could not sign URL: {e}")
    return url


def _parse_metadata(raw: Any) -> dict:
    """Parse a JSON string into a dict, returning empty dict on failure."""
    if not raw:
        return {}
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return {}


# ==========================================
#  CHAT ENDPOINTS
# ==========================================


@router.post("/chat", response_model=ChatResponse, tags=["chat"])
async def chat(
    request: ChatRequest,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, Any]:
    """Process a natural language question through the NL2SQL pipeline."""
    orchestrator = None
    try:
        orchestrator = PipelineOrchestrator(settings)
        return await orchestrator.process(request.message, request.user_id)
    except Exception as e:
        logger.error(f"Error processing chat request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        if orchestrator:
            try:
                await orchestrator.close()
            except Exception as cleanup_error:
                logger.warning(f"Error during cleanup: {cleanup_error}")


@router.post("/chat/stream", response_class=StreamingResponse, tags=["chat"])
async def chat_stream(
    request: ChatRequest,
    settings: Settings = Depends(get_settings_dependency),
) -> StreamingResponse:
    """Stream pipeline results as Server-Sent Events."""

    async def generate() -> AsyncIterator[str]:
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
                except Exception as cleanup_error:
                    logger.warning(f"Error during cleanup: {cleanup_error}")

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


# ==========================================
#  CACHE ENDPOINTS
# ==========================================


@router.get("/cache/stats", tags=["cache"])
async def get_cache_stats() -> dict[str, Any]:
    """Return cache hit/miss statistics."""
    return SemanticCache.get_stats()


@router.delete("/cache", tags=["cache"])
async def clear_cache() -> dict[str, str]:
    """Invalidate all cached SQL generation results."""
    SemanticCache.clear()
    logger.info("Cache cleared by API request")
    return {"message": "Cache cleared successfully", "status": "success"}


# ==========================================
#  PROJECT ENDPOINTS
# ==========================================


@router.get("/projects", response_model=list[Project], tags=["projects"])
async def get_projects(settings: Settings = Depends(get_settings_dependency)) -> list[Project]:
    """List all projects."""
    sql = "SELECT id, title, description, owner, createdAt as created_at FROM dbo.Projects ORDER BY createdAt DESC"
    try:
        projects_data = await execute_query(settings, sql)
        return [
            Project(
                id=str(p.get("id", "")),
                title=str(p.get("title", "")),
                description=p.get("description"),
                owner=p.get("owner"),
                created_at=p.get("created_at"),
                items=[],
            )
            for p in projects_data
        ]
    except Exception as e:
        logger.error(f"Error fetching projects: {e}")
        return []


@router.post("/projects", response_model=Project, tags=["projects"])
async def create_project(
    request: CreateProjectRequest, settings: Settings = Depends(get_settings_dependency)
) -> Project:
    """Create a new project."""
    new_id = str(uuid.uuid4())
    sql = """
    INSERT INTO dbo.Projects (id, title, description, owner, createdAt)
    VALUES (?, ?, ?, ?, GETDATE())
    """
    result = await execute_insert(settings, sql, (new_id, request.title, request.description, request.owner))
    if not result.get("success") or result.get("error"):
        raise HTTPException(status_code=500, detail=f"Database error: {result.get('error', 'Unknown error')}")

    return Project(
        id=new_id,
        title=request.title,
        description=request.description,
        owner=request.owner,
        items=[],
    )


@router.post("/projects/{project_id}/items", tags=["projects"])
async def add_project_item(
    project_id: str,
    request: AddProjectItemRequest,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, str]:
    """Add a graph to a project."""
    item_id = str(uuid.uuid4())
    MAX_TITLE_LENGTH = 200

    title = (request.user_question or request.title or "Nueva Gráfica").strip()
    if len(title) > MAX_TITLE_LENGTH:
        title = title[: MAX_TITLE_LENGTH - 3] + "..."

    sql = """
    INSERT INTO dbo.ProjectItems (id, projectId, type, content, title, createdAt)
    VALUES (?, ?, ?, ?, ?, GETDATE())
    """
    result = await execute_insert(
        settings, sql, (item_id, project_id, request.type, _clean_blob_url(request.content), title)
    )
    if not result.get("success") or result.get("error"):
        raise HTTPException(status_code=500, detail=f"Database error: {result.get('error', 'Unknown error')}")

    return {"status": "success", "id": item_id}


@router.get("/projects/{project_id}/items", tags=["projects"])
async def get_project_items(
    project_id: str, settings: Settings = Depends(get_settings_dependency)
) -> list[dict[str, Any]]:
    """List all items for a project, with signed blob URLs."""
    sql = """
    SELECT id, projectId, type, content, title, createdAt as created_at
    FROM dbo.ProjectItems
    WHERE projectId = ?
    ORDER BY createdAt DESC
    """
    try:
        items_data = await execute_query(settings, sql, (project_id,))
        storage_client = BlobStorageClient(settings)
        container_name = settings.azure_storage_container_name or "charts"

        items = [
            {
                "id": str(item.get("id")),
                "project_id": str(item.get("projectId")),
                "type": item.get("type"),
                "content": await _sign_blob_url(item.get("content", ""), storage_client, container_name),
                "title": item.get("title"),
                "created_at": item.get("created_at"),
            }
            for item in items_data
        ]

        await storage_client.close()
        return items
    except Exception as e:
        logger.error(f"Error fetching project items: {e}")
        return []


# ==========================================
#  GRAPH ENDPOINTS
# ==========================================


@router.get("/graphs", response_model=list[Graph], tags=["graphs"])
async def get_graphs(
    user_id: str | None = None,
    settings: Settings = Depends(get_settings_dependency),
) -> list[Graph]:
    """List saved graphs, optionally filtered by user_id."""
    base_sql = """
    SELECT id, type, content, title, query, createdAt as created_at, metadata, user_id
    FROM dbo.Graphs
    """
    if user_id:
        graphs_data = await execute_query(settings, base_sql + "WHERE user_id = ? ORDER BY createdAt DESC", (user_id,))
    else:
        graphs_data = await execute_query(settings, base_sql + "ORDER BY createdAt DESC")

    storage_client = BlobStorageClient(settings)
    container_name = settings.azure_storage_container_name or "charts"

    graphs = [
        Graph(
            id=str(g.get("id", "")),
            type=str(g.get("type", "")),
            content=await _sign_blob_url(g.get("content", ""), storage_client, container_name),
            title=str(g.get("title", "")),
            query=g.get("query"),
            created_at=g.get("created_at"),
            metadata=_parse_metadata(g.get("metadata")),
            user_id=g.get("user_id"),
        )
        for g in graphs_data
    ]

    await storage_client.close()
    return graphs


@router.post("/graphs", tags=["graphs"])
async def save_graph(
    request: SaveGraphRequest,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, str]:
    """Save a graph from chat."""
    graph_id = str(uuid.uuid4())
    metadata_str = json.dumps(request.metadata) if request.metadata else None

    sql = """
    INSERT INTO dbo.Graphs (id, type, content, title, query, metadata, user_id, createdAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
    """
    result = await execute_insert(
        settings, sql,
        (graph_id, request.type, _clean_blob_url(request.content), request.title, request.query, metadata_str, request.user_id),
    )
    if not result.get("success") or result.get("error"):
        raise HTTPException(status_code=500, detail=f"Database error: {result.get('error', 'Unknown error')}")

    return {"status": "success", "id": graph_id}


@router.delete("/graphs/{graph_id}", tags=["graphs"])
async def delete_graph(
    graph_id: str,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, str]:
    """Delete a single graph."""
    result = await execute_insert(settings, "DELETE FROM dbo.Graphs WHERE id = ?", (graph_id,))
    if not result.get("success") or result.get("error"):
        raise HTTPException(status_code=500, detail=f"Database error: {result.get('error', 'Unknown error')}")
    return {"status": "success", "id": graph_id}


@router.delete("/graphs", tags=["graphs"])
async def delete_graphs_bulk(
    request: DeleteGraphsRequest,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, Any]:
    """Delete multiple graphs at once."""
    if not request.graph_ids:
        raise HTTPException(status_code=400, detail="No graph IDs provided")

    placeholders = ", ".join(["?" for _ in request.graph_ids])
    result = await execute_insert(
        settings, f"DELETE FROM dbo.Graphs WHERE id IN ({placeholders})", tuple(request.graph_ids)
    )
    if not result.get("success") or result.get("error"):
        raise HTTPException(status_code=500, detail=f"Database error: {result.get('error', 'Unknown error')}")

    return {"status": "success", "deleted_count": len(request.graph_ids)}

@router.patch("/graphs/{graph_id}/refresh", tags=["graphs"])
async def refresh_graph(
    graph_id: str,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, Any]:
    """Re-execute a graph's stored query and regenerate its visualization."""
    # 1. Fetch graph from DB
    graphs_data = await execute_query(
        settings,
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
    orchestrator = None
    try:
        orchestrator = PipelineOrchestrator(settings)
        result = await orchestrator.refresh_graph(
            sql=sql_query,
            chart_type=chart_type,
            title=title,
            user_id=user_id,
        )
    except Exception as e:
        logger.error(f"Error refreshing graph {graph_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        if orchestrator:
            try:
                await orchestrator.close()
            except Exception as cleanup_error:
                logger.warning(f"Error during cleanup: {cleanup_error}")

    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])

    # 3. Update graph in DB with new content
    new_content = _clean_blob_url(result.get("content", ""))
    update_result = await execute_insert(
        settings,
        "UPDATE dbo.Graphs SET content = ?, createdAt = GETDATE() WHERE id = ?",
        (new_content, graph_id),
    )
    if not update_result.get("success") or update_result.get("error"):
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update graph: {update_result.get('error', 'Unknown error')}",
        )

    # 4. Return signed URL
    storage_client = BlobStorageClient(settings)
    container_name = settings.azure_storage_container_name or "charts"
    signed_url = await _sign_blob_url(new_content, storage_client, container_name)
    await storage_client.close()

    return {
        "status": "success",
        "id": graph_id,
        "content": signed_url,
        "row_count": result.get("row_count", 0),
    }

# ==========================================
#  INFORME ENDPOINTS
# ==========================================


@router.get("/informes", response_model=list[InformeSummary], tags=["informes"])
async def list_informes(
    owner: str | None = None,
    settings: Settings = Depends(get_settings_dependency),
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
    """
    if owner:
        rows = await execute_query(settings, sql.format(where="WHERE p.owner = ?"), (owner,))
    else:
        rows = await execute_query(settings, sql.format(where=""))

    return [
        InformeSummary(
            id=str(r["id"]),
            title=str(r["title"]),
            description=r.get("description"),
            owner=r.get("owner"),
            created_at=r.get("created_at"),
            graph_count=r.get("graph_count", 0),
        )
        for r in rows
    ]


@router.post("/informes", response_model=InformeSummary, tags=["informes"])
async def create_informe(
    request: CreateInformeRequest,
    settings: Settings = Depends(get_settings_dependency),
) -> InformeSummary:
    """Create a new informe."""
    new_id = str(uuid.uuid4())
    result = await execute_insert(
        settings,
        "INSERT INTO dbo.Projects (id, title, description, owner, createdAt) VALUES (?, ?, ?, ?, GETDATE())",
        (new_id, request.title, request.description, request.owner),
    )
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=f"Database error: {result.get('error', 'Unknown')}")

    return InformeSummary(id=new_id, title=request.title, description=request.description, owner=request.owner, graph_count=0)


@router.get("/informes/{informe_id}", response_model=InformeDetail, tags=["informes"])
async def get_informe(
    informe_id: str,
    settings: Settings = Depends(get_settings_dependency),
) -> InformeDetail:
    """Get informe detail with all its graphs (signed URLs)."""
    header_rows = await execute_query(
        settings,
        "SELECT id, title, description, owner, createdAt AS created_at FROM dbo.Projects WHERE id = ?",
        (informe_id,),
    )
    if not header_rows:
        raise HTTPException(status_code=404, detail="Informe not found")
    h = header_rows[0]

    graphs_data = await execute_query(
        settings,
        """
        SELECT pi.id AS item_id, g.id AS graph_id, g.type, g.content, g.title, g.query, g.createdAt AS created_at
        FROM dbo.ProjectItems pi
        INNER JOIN dbo.Graphs g ON pi.graph_id = g.id
        WHERE pi.projectId = ?
        ORDER BY pi.createdAt ASC
        """,
        (informe_id,),
    )

    storage_client = BlobStorageClient(settings)
    container = settings.azure_storage_container_name or "charts"

    graphs = [
        InformeGraph(
            item_id=str(row["item_id"]),
            graph_id=str(row["graph_id"]),
            type=str(row.get("type", "")),
            content=await _sign_blob_url(row.get("content", ""), storage_client, container),
            title=str(row.get("title", "")),
            query=row.get("query"),
            created_at=row.get("created_at"),
        )
        for row in graphs_data
    ]
    await storage_client.close()

    return InformeDetail(
        id=str(h["id"]), title=str(h["title"]), description=h.get("description"),
        owner=h.get("owner"), created_at=h.get("created_at"), graphs=graphs,
    )


@router.delete("/informes/{informe_id}", tags=["informes"])
async def delete_informe(
    informe_id: str,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, str]:
    """Delete an informe and its graph associations (not the graphs themselves)."""
    await execute_insert(settings, "DELETE FROM dbo.ProjectItems WHERE projectId = ?", (informe_id,))
    result = await execute_insert(settings, "DELETE FROM dbo.Projects WHERE id = ?", (informe_id,))
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=f"Database error: {result.get('error', 'Unknown')}")
    return {"status": "success", "id": informe_id}


@router.post("/informes/{informe_id}/graphs", tags=["informes"])
async def add_graphs_to_informe(
    informe_id: str,
    request: AddGraphsToInformeRequest,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, Any]:
    """Add one or more graphs to an informe."""
    if not request.graph_ids:
        raise HTTPException(status_code=400, detail="No graph IDs provided")

    # Verify informe exists
    if not await execute_query(settings, "SELECT id FROM dbo.Projects WHERE id = ?", (informe_id,)):
        raise HTTPException(status_code=404, detail="Informe not found")

    # Find which graphs exist and which are already linked
    ph = ", ".join(["?" for _ in request.graph_ids])
    existing = await execute_query(settings, f"SELECT id, title FROM dbo.Graphs WHERE id IN ({ph})", tuple(request.graph_ids))
    existing_map = {str(g["id"]): str(g["title"]) for g in existing}

    already = await execute_query(
        settings,
        f"SELECT graph_id FROM dbo.ProjectItems WHERE projectId = ? AND graph_id IN ({ph})",
        (informe_id, *request.graph_ids),
    )
    already_ids = {str(r["graph_id"]) for r in already}

    added, skipped, not_found = [], [], []

    for gid in request.graph_ids:
        if gid not in existing_map:
            not_found.append(gid)
        elif gid in already_ids:
            skipped.append(gid)
        else:
            result = await execute_insert(
                settings,
                "INSERT INTO dbo.ProjectItems (id, projectId, type, content, title, graph_id, createdAt) VALUES (?, ?, 'graph', '', ?, ?, GETDATE())",
                (str(uuid.uuid4()), informe_id, existing_map[gid], gid),
            )
            if result.get("success"):
                added.append(gid)

    return {"status": "success", "added": added, "skipped_duplicates": skipped, "not_found": not_found}


@router.delete("/informes/{informe_id}/graphs/{item_id}", tags=["informes"])
async def remove_graph_from_informe(
    informe_id: str,
    item_id: str,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, str]:
    """Remove a graph from an informe (does NOT delete the graph itself)."""
    result = await execute_insert(
        settings, "DELETE FROM dbo.ProjectItems WHERE id = ? AND projectId = ?", (item_id, informe_id),
    )
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=f"Database error: {result.get('error', 'Unknown')}")
    return {"status": "success", "item_id": item_id}


@router.patch("/informes/{informe_id}/refresh", tags=["informes"])
async def refresh_informe(
    informe_id: str,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, Any]:
    """Refresh all graphs in an informe by re-executing their stored queries."""
    items = await execute_query(
        settings,
        "SELECT graph_id FROM dbo.ProjectItems WHERE projectId = ? AND graph_id IS NOT NULL",
        (informe_id,),
    )
    if not items:
        raise HTTPException(status_code=404, detail="Informe has no graphs")

    refreshed, failed, skipped = [], [], []
    orchestrator = PipelineOrchestrator(settings)

    try:
        for item in items:
            gid = str(item["graph_id"])
            graph_rows = await execute_query(
                settings, "SELECT type, title, query, user_id FROM dbo.Graphs WHERE id = ?", (gid,),
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
                    sql=g["query"], chart_type=g.get("type", "bar"),
                    title=g.get("title", "Visualización"), user_id=g.get("user_id", "system"),
                )
                if result.get("error"):
                    failed.append({"id": gid, "error": result["error"]})
                    continue

                await execute_insert(
                    settings,
                    "UPDATE dbo.Graphs SET content = ?, createdAt = GETDATE() WHERE id = ?",
                    (_clean_blob_url(result.get("content", "")), gid),
                )
                refreshed.append(gid)
            except Exception as e:
                logger.error(f"Error refreshing graph {gid}: {e}")
                failed.append({"id": gid, "error": str(e)})
    finally:
        try:
            await orchestrator.close()
        except Exception:
            pass

    return {"status": "success", "informe_id": informe_id, "refreshed": refreshed, "failed": failed, "skipped": skipped}

@router.delete("/informes", tags=["informes"])
async def delete_informes_bulk(
    request: DeleteInformesRequest,
    settings: Settings = Depends(get_settings_dependency),
) -> dict[str, Any]:
    """Delete multiple informes and their associations."""
    if not request.informe_ids:
        raise HTTPException(status_code=400, detail="No informe IDs provided")

    placeholders = ", ".join(["?" for _ in request.informe_ids])
    await execute_insert(
        settings,
        f"DELETE FROM dbo.ProjectItems WHERE projectId IN ({placeholders})",
        tuple(request.informe_ids),
    )
    result = await execute_insert(
        settings,
        f"DELETE FROM dbo.Projects WHERE id IN ({placeholders})",
        tuple(request.informe_ids),
    )
    if not result.get("success") or result.get("error"):
        raise HTTPException(status_code=500, detail=f"Database error: {result.get('error', 'Unknown error')}")

    return {"status": "success", "deleted_count": len(request.informe_ids)}