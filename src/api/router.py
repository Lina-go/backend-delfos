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
    Graph,
    HealthResponse,
    Project,
    SaveGraphRequest,
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