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
    HealthResponse,
    Project,
)
from src.config.settings import Settings
from src.infrastructure.cache.semantic_cache import SemanticCache
from src.infrastructure.database.connection import execute_insert, execute_query
from src.infrastructure.storage.blob_client import BlobStorageClient
from src.orchestrator.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)

router = APIRouter()

# ==========================================
#  CHAT ENDPOINT
# ==========================================


@router.post("/chat", response_model=ChatResponse, tags=["chat"])
async def chat(
    request: ChatRequest,
    settings: Settings = Depends(get_settings_dependency),  # noqa: B008
) -> dict[str, Any]:
    """
    Main chat endpoint for natural language to SQL pipeline.

    Processes the user's question through the complete pipeline:
    1. Triage
    2. Intent classification
    3. Schema selection
    4. SQL generation & execution
    5. Verification
    6. Visualization (if needed)
    7. Response formatting
    """
    orchestrator = None
    try:
        orchestrator = PipelineOrchestrator(settings)
        response = await orchestrator.process(request.message, request.user_id)
        return response
    except Exception as e:
        logger.error(f"Error processing chat request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        # Cleanup resources
        if orchestrator:
            try:
                await orchestrator.close()
            except Exception as cleanup_error:
                logger.warning(f"Error during cleanup: {cleanup_error}")


@router.post(
    "/chat/stream",
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Server-Sent Events stream",
            "content": {
                "text/event-stream": {
                    "schema": {
                        "type": "string",
                        "example": 'data: {"step": "triage", "result": {...}, "state": {...}}\n\n',
                    }
                }
            },
        }
    },
    tags=["chat"],
)
async def chat_stream(
    request: ChatRequest,
    settings: Settings = Depends(get_settings_dependency),  # noqa: B008
) -> StreamingResponse:
    """
    Streaming chat endpoint using Server-Sent Events (SSE).

    Emits events as each pipeline step completes:
    - triage: Triage classification result
    - intent: Intent classification result
    - schema: Schema selection result
    - sql_generation: SQL generation result
    - sql_execution: SQL execution result
    - verification: Verification result
    - visualization: Visualization result (if required)
    - graph: Graph generation result (if visualization)
    - format: Response formatting result
    - complete: Final response
    - error: Error event (if any)

    **Note**: This endpoint returns a streaming response. Use EventSource or fetch with streaming
    to consume the events in real-time.
    """

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
                    logger.info("Stream connection closed")
                except Exception as cleanup_error:
                    logger.warning(f"Error during cleanup: {cleanup_error}")

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Health check endpoint."""
    return HealthResponse(status="healthy", version="0.1.0")


# ==========================================
#  CACHE MANAGEMENT ENDPOINTS
# ==========================================


@router.get("/cache/stats", tags=["cache"])
async def get_cache_stats() -> dict[str, Any]:
    """
    Get cache statistics.

    Returns:
        Dictionary with cache statistics:
        - size: Number of cached entries
        - hits: Number of cache hits
        - misses: Number of cache misses
        - hit_rate: Hit rate as percentage
    """
    stats = SemanticCache.get_stats()
    return stats


@router.delete("/cache", tags=["cache"])
async def clear_cache() -> dict[str, str]:
    """
    Clear all cached SQL generation results.

    Use this endpoint when you want to invalidate the cache,
    for example after schema changes or when you want fresh results.

    Returns:
        Success message
    """
    SemanticCache.clear()
    logger.info("Cache cleared by API request")
    return {"message": "Cache cleared successfully", "status": "success"}


# ==========================================
#  PROJECT MANAGEMENT ENDPOINTS
# ==========================================


@router.get("/projects", response_model=list[Project], tags=["projects"])
async def get_projects(settings: Settings = Depends(get_settings_dependency)) -> list[Project]:
    """
    Get all projects.
    Uses direct database connection to retrieve the list from the DB.
    """
    sql = "SELECT id, title, description, owner, createdAt as created_at FROM dbo.Projects ORDER BY createdAt DESC"

    try:
        projects_data = await execute_query(settings, sql)

        projects = []
        for p in projects_data:
            projects.append(
                Project(
                    id=str(p.get("id", "")),
                    title=str(p.get("title", "")),
                    description=p.get("description"),
                    owner=p.get("owner"),
                    created_at=p.get("created_at"),
                    items=[],
                )
            )
        return projects
    except Exception as e:
        logger.error(f"Error fetching projects: {e}")
        return []


@router.post("/projects", response_model=Project, tags=["projects"])
async def create_project(
    request: CreateProjectRequest, settings: Settings = Depends(get_settings_dependency)
) -> Project:
    """
    Create a new project using direct SQL insertion (FAST, No LLM).
    """
    new_id = str(uuid.uuid4())

    sql = """
    INSERT INTO dbo.Projects (id, title, description, owner, createdAt)
    VALUES (?, ?, ?, ?, GETDATE())
    """
    params = (new_id, request.title, request.description, request.owner)

    result = await execute_insert(settings, sql, params)
    if not result.get("success") or result.get("error"):
        raise HTTPException(
            status_code=500, detail=f"Database error: {result.get('error', 'Unknown error')}"
        )

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
    """
    Add a graph (URL) to a project using direct SQL insertion (FAST, No LLM).
    The title will always be the user's question (user_question field).
    """
    item_id = str(uuid.uuid4())

    MAX_TITLE_LENGTH = 200

    if request.user_question:
        title_to_use = request.user_question.strip()
    elif request.title:
        title_to_use = request.title.strip()
    else:
        title_to_use = "Nueva Gráfica"

    # Truncate title if necessary to fit DB column
    if len(title_to_use) > MAX_TITLE_LENGTH:
        safe_title = title_to_use[: MAX_TITLE_LENGTH - 3] + "..."
        logger.warning(f"Title truncated from {len(title_to_use)} to {MAX_TITLE_LENGTH} characters")
    else:
        safe_title = title_to_use

    # Clean URL: Remove SAS token if present before saving to database
    content_to_save = request.content
    if content_to_save and "blob.core.windows.net" in content_to_save:
        try:
            parsed = urlparse(content_to_save)
            # Reconstruct URL without the query string (SAS token)
            content_to_save = urlunparse(parsed._replace(query=""))
            logger.debug(f"Cleaned URL before saving: removed SAS token")
        except Exception as e:
            logger.warning(f"Could not clean URL: {e}, saving as-is")

    # Use parameterized query to prevent SQL injection
    sql = """
    INSERT INTO dbo.ProjectItems (id, projectId, type, content, title, createdAt)
    VALUES (?, ?, ?, ?, ?, GETDATE())
    """
    params = (item_id, project_id, request.type, content_to_save, safe_title)

    # Execute directly via database connection
    result = await execute_insert(settings, sql, params)
    if not result.get("success") or result.get("error"):
        raise HTTPException(
            status_code=500, detail=f"Database error: {result.get('error', 'Unknown error')}"
        )

    return {"status": "success", "id": item_id}


@router.get("/projects/{project_id}/items", tags=["projects"])
async def get_project_items(
    project_id: str, settings: Settings = Depends(get_settings_dependency)
) -> list[dict[str, Any]]:
    """
    Get all items (graphs) for a specific project.
    """
    sql = """
    SELECT id, projectId, type, content, title, createdAt as created_at
    FROM dbo.ProjectItems
    WHERE projectId = ?
    ORDER BY createdAt DESC
    """

    try:
        items_data = await execute_query(settings, sql, (project_id,))

        # Sign blob URLs dynamically when retrieving
        storage_client = BlobStorageClient(settings)
        container_name = settings.azure_storage_container_name or "charts"

        items = []
        for item in items_data:
            content_url = item.get("content")

            # Sign URL if it's a blob URL
            if content_url and "blob.core.windows.net" in content_url:
                try:
                    # Remove any existing SAS token from URL before parsing
                    url_to_parse = content_url.split("?")[0]
                    parsed = urlparse(url_to_parse)
                    # Extract blob name from path (format: /container/blob-name)
                    path_parts = parsed.path.strip("/").split("/", 1)
                    if len(path_parts) > 1:
                        blob_name = path_parts[1]
                        # Generate new SAS token valid for 1 hour
                        content_url = await storage_client.get_blob_sas_url(
                            container_name=container_name,
                            blob_name=blob_name,
                        )
                        logger.debug(f"Signed project item URL for blob: {blob_name}")
                    else:
                        logger.warning(f"Could not extract blob name from project item URL: {content_url}")
                except Exception as e:
                    logger.error(f"Error signing URL for project item: {e}", exc_info=True)

            items.append(
                {
                    "id": str(item.get("id")),
                    "project_id": str(item.get("projectId")),
                    "type": item.get("type"),
                    "content": content_url,  # Use signed URL
                    "title": item.get("title"),
                    "created_at": item.get("created_at"),
                }
            )

        await storage_client.close()
        return items
    except Exception as e:
        logger.error(f"Error fetching project items: {e}")
        return []
