"""Informe endpoints."""

from typing import Any

from fastapi import APIRouter, Depends, Query

from src.api.models import (
    AddGraphsToInformeRequest,
    BulkOperationResponse,
    CreateInformeRequest,
    DeleteInformesRequest,
    InformeDetail,
    InformeSummary,
    OperationResponse,
)
from src.config.constants import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from src.config.settings import Settings, get_settings
from src.services.informes import InformeService

router = APIRouter()


@router.get("/", response_model=list[InformeSummary])
async def list_informes(
    owner: str | None = None,
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(0, ge=0),
    settings: Settings = Depends(get_settings),
) -> list[InformeSummary]:
    """List all informes with graph count."""
    svc = InformeService(settings)
    return await svc.list_informes(owner, offset, limit)


@router.post("/", response_model=InformeSummary, status_code=201)
async def create_informe(
    request: CreateInformeRequest,
    settings: Settings = Depends(get_settings),
) -> InformeSummary:
    """Create a new informe."""
    svc = InformeService(settings)
    return await svc.create_informe(request)


@router.get("/{informe_id}", response_model=InformeDetail)
async def get_informe(
    informe_id: str,
    settings: Settings = Depends(get_settings),
) -> InformeDetail:
    """Get informe detail with all its graphs."""
    svc = InformeService(settings)
    return await svc.get_informe(informe_id)


@router.delete("/{informe_id}", response_model=OperationResponse)
async def delete_informe(
    informe_id: str,
    settings: Settings = Depends(get_settings),
) -> OperationResponse:
    """Delete an informe and its graph associations (not the graphs themselves)."""
    svc = InformeService(settings)
    await svc.delete_informe(informe_id)
    return OperationResponse(id=informe_id)


@router.post("/{informe_id}/graphs", status_code=201)
async def add_graphs_to_informe(
    informe_id: str,
    request: AddGraphsToInformeRequest,
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """Add one or more graphs to an informe."""
    svc = InformeService(settings)
    return await svc.add_graphs(informe_id, request.graph_ids)


@router.delete(
    "/{informe_id}/graphs/{item_id}",
    response_model=OperationResponse,
)
async def remove_graph_from_informe(
    informe_id: str,
    item_id: str,
    settings: Settings = Depends(get_settings),
) -> OperationResponse:
    """Remove a graph from an informe (does NOT delete the graph itself)."""
    svc = InformeService(settings)
    await svc.remove_graph(informe_id, item_id)
    return OperationResponse(id=item_id)


@router.patch("/{informe_id}/refresh")
async def refresh_informe(
    informe_id: str,
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """Refresh all graphs in an informe by re-executing their stored queries."""
    svc = InformeService(settings)
    return await svc.refresh_informe(informe_id)


@router.delete("/", response_model=BulkOperationResponse)
async def delete_informes_bulk(
    request: DeleteInformesRequest,
    settings: Settings = Depends(get_settings),
) -> BulkOperationResponse:
    """Delete multiple informes and their associations."""
    svc = InformeService(settings)
    deleted_count = await svc.delete_bulk(request.informe_ids)
    return BulkOperationResponse(deleted_count=deleted_count)
