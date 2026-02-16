"""Project endpoints."""

from typing import Any

from fastapi import APIRouter, Depends, Query

from src.api.models import (
    AddProjectItemRequest,
    CreateProjectRequest,
    OperationResponse,
    Project,
)
from src.config.constants import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from src.config.settings import Settings, get_settings
from src.services.projects import ProjectService

router = APIRouter()


@router.get("/", response_model=list[Project])
async def get_projects(
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(0, ge=0),
    settings: Settings = Depends(get_settings),
) -> list[Project]:
    """List all projects."""
    svc = ProjectService(settings)
    return await svc.list_projects(offset, limit)


@router.post("/", response_model=Project, status_code=201)
async def create_project(
    request: CreateProjectRequest,
    settings: Settings = Depends(get_settings),
) -> Project:
    """Create a new project."""
    svc = ProjectService(settings)
    return await svc.create_project(request)


@router.post(
    "/{project_id}/items",
    response_model=OperationResponse,
    status_code=201,
)
async def add_project_item(
    project_id: str,
    request: AddProjectItemRequest,
    settings: Settings = Depends(get_settings),
) -> OperationResponse:
    """Add a graph to a project."""
    svc = ProjectService(settings)
    item_id = await svc.add_item(project_id, request)
    return OperationResponse(id=item_id)


@router.get("/{project_id}/items")
async def get_project_items(
    project_id: str,
    settings: Settings = Depends(get_settings),
) -> list[dict[str, Any]]:
    """List all items for a project."""
    svc = ProjectService(settings)
    return await svc.get_items(project_id)
