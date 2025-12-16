"""API routes."""

import json
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from src.api.dependencies import get_settings_dependency
from src.api.models import ChatRequest, ChatResponse, HealthResponse, Project, CreateProjectRequest, AddProjectItemRequest
from src.config.settings import Settings
from src.infrastructure.cache.semantic_cache import SemanticCache
from src.infrastructure.mcp.client import MCPClient
from src.orchestrator.pipeline import PipelineOrchestrator
from src.services.sql.executor import SQLExecutor

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
async def get_projects(
    settings: Settings = Depends(get_settings_dependency)
) -> list[Project]:
    """
    Get all projects. 
    Uses SQLExecutor to retrieve and automatically format the list from the DB.
    """
    executor = SQLExecutor(settings)
    
    # Simple Query. The Executor will format the result into JSON for us.
    sql = "SELECT id, title, description, owner, createdAt as created_at FROM dbo.Projects ORDER BY createdAt DESC"
    
    try:
        # 1. Execute and get structured JSON
        result = await executor.execute(sql)
        projects_data = result.get("resultados", [])
        
        # 2. Convert to Pydantic Models
        projects = []
        for p in projects_data:
            projects.append(Project(
                id=str(p.get("id")),
                title=p.get("title"),
                description=p.get("description"),
                owner=p.get("owner"),
                created_at=p.get("created_at"), 
                items=[] # Items loaded separately or lazily if needed
            ))
        return projects
    except Exception as e:
        logger.error(f"Error fetching projects: {e}")
        return []

@router.post("/projects", response_model=Project, tags=["projects"])
async def create_project(
    request: CreateProjectRequest,
    settings: Settings = Depends(get_settings_dependency)
) -> Project:
    """
    Create a new project using direct SQL insertion (FAST, No LLM).
    """
    new_id = str(uuid.uuid4())
    
    # 1. Prepare SQL
    sql = f"""
    INSERT INTO dbo.Projects (id, title, description, owner, createdAt)
    VALUES ('{new_id}', '{request.title}', '{request.description}', '{request.owner}', GETDATE())
    """
    
    # 2. Execute Directly
    async with MCPClient(settings) as client:
        result = await client.execute_sql(sql)
        if result.get("error"):
            raise HTTPException(status_code=500, detail=f"Database error: {result['error']}")

    # 3. Return the object so frontend can update immediately
    return Project(
        id=new_id,
        title=request.title,
        description=request.description,
        owner=request.owner,
        items=[]
    )

@router.post("/projects/{project_id}/items", tags=["projects"])
async def add_project_item(
    project_id: str,
    request: AddProjectItemRequest,
    settings: Settings = Depends(get_settings_dependency)
) -> dict[str, str]:
    """
    Add a graph (URL) to a project using direct SQL insertion (FAST, No LLM).
    """
    item_id = str(uuid.uuid4())
    
    # Escape quotes to prevent SQL errors
    safe_content = request.content.replace("'", "''") 
    safe_title = request.title.replace("'", "''") if request.title else "Untitled"
    
    # 1. Prepare SQL
    sql = f"""
    INSERT INTO dbo.ProjectItems (id, projectId, type, content, title, createdAt)
    VALUES ('{item_id}', '{project_id}', '{request.type}', '{safe_content}', '{safe_title}', GETDATE())
    """
    
    # 2. Execute Directly
    async with MCPClient(settings) as client:
        result = await client.execute_sql(sql)
        if result.get("error"):
            raise HTTPException(status_code=500, detail=f"Database error: {result['error']}")
            
    return {"status": "success", "id": item_id}
