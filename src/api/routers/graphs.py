"""Graph endpoints."""

from fastapi import APIRouter, Depends, Query

from src.api.models import (
    BulkOperationResponse,
    DeleteGraphsRequest,
    Graph,
    OperationResponse,
    SaveGraphRequest,
)
from src.config.constants import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from src.config.settings import Settings, get_settings
from src.services.graphs import GraphService

router = APIRouter()


@router.get("/", response_model=list[Graph])
async def get_graphs(
    user_id: str | None = None,
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(0, ge=0),
    settings: Settings = Depends(get_settings),
) -> list[Graph]:
    """List saved graphs, optionally filtered by user_id."""
    svc = GraphService(settings)
    return await svc.list_graphs(user_id, offset, limit)


@router.post("/", response_model=OperationResponse, status_code=201)
async def save_graph(
    request: SaveGraphRequest,
    settings: Settings = Depends(get_settings),
) -> OperationResponse:
    """Save a graph from chat."""
    svc = GraphService(settings)
    graph_id = await svc.save_graph(request)
    return OperationResponse(id=graph_id)


@router.delete("/{graph_id}", response_model=OperationResponse)
async def delete_graph(
    graph_id: str,
    settings: Settings = Depends(get_settings),
) -> OperationResponse:
    """Delete a single graph."""
    svc = GraphService(settings)
    await svc.delete_graph(graph_id)
    return OperationResponse(id=graph_id)


@router.delete("/", response_model=BulkOperationResponse)
async def delete_graphs_bulk(
    request: DeleteGraphsRequest,
    settings: Settings = Depends(get_settings),
) -> BulkOperationResponse:
    """Delete multiple graphs at once."""
    svc = GraphService(settings)
    deleted_count = await svc.delete_bulk(request.graph_ids)
    return BulkOperationResponse(deleted_count=deleted_count)


@router.patch("/{graph_id}/refresh")
async def refresh_graph(
    graph_id: str,
    settings: Settings = Depends(get_settings),
) -> Graph:
    """Re-execute a graph's stored query and regenerate its visualization."""
    svc = GraphService(settings)
    return await svc.refresh_graph(graph_id)
