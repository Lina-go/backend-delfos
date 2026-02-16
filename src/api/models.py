"""Request/Response models for API endpoints."""

import json
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


def _parse_json_field(value: Any) -> dict[str, Any]:
    """Parse a JSON string/dict field, returning {} on failure."""
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        result = json.loads(value)
        return result if isinstance(result, dict) else {}
    except (json.JSONDecodeError, ValueError):
        return {}


class ChatRequest(BaseModel):
    """Request model for chat endpoint."""

    message: str = Field(
        ..., min_length=1, max_length=5000, description="User's natural language question"
    )
    user_id: str = Field(..., min_length=1, max_length=100, description="User identifier")


class ChatResponse(BaseModel):
    """Response model for chat endpoint."""

    patron: str = Field(..., description="Analytical pattern")
    datos: list[dict[str, Any]] = Field(..., description="SQL query results")
    arquetipo: str | None = Field(None, description="Pattern type (A-K)")
    visualizacion: str = Field(..., description="YES or NO")
    tipo_grafica: str | None = Field(None, description="Chart type")
    titulo_grafica: str | None = Field(None, description="Chart title")
    data_points: list[dict[str, Any]] | None = Field(None, description="Formatted data points")
    metric_name: str | None = Field(None, description="Metric name")
    x_axis_name: str | None = Field(None, description="Label for X axis")
    y_axis_name: str | None = Field(None, description="Label for Y axis")
    series_name: str | None = Field(None, description="Label for series grouping")
    category_name: str | None = Field(None, description="Label for category grouping")
    is_tasa: bool = Field(False, description="Whether the question involves interest rates")
    link_power_bi: str | None = Field(None, description="Power BI URL")
    insight: str | None = Field(None, description="Generated insight")
    sql_query: str | None = Field(None, description="Generated SQL query")
    error: str | None = Field(
        "", description="Error message if error occurred, empty string otherwise"
    )
    needs_clarification: bool = Field(
        False, description="Whether the system needs more information from the user"
    )
    clarification_question: str | None = Field(
        None, description="Question to ask the user for clarification"
    )


class HealthResponse(BaseModel):
    """Response model for health endpoint."""

    status: str = Field(..., description="Service status")
    version: str = Field(..., description="Application version")


class ProjectItem(BaseModel):
    """Item stored in a project."""

    id: str
    type: str
    content: str
    title: str | None = None
    created_at: datetime | None = None
    metadata: dict[str, Any] | None = None


class Project(BaseModel):
    """Project definition."""

    id: str
    title: str
    description: str | None = None
    owner: str | None = None
    created_at: datetime | None = None
    items: list[ProjectItem] = []

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "Project":
        return cls(
            id=str(row.get("id", "")),
            title=str(row.get("title", "")),
            description=row.get("description"),
            owner=row.get("owner"),
            created_at=row.get("created_at"),
            items=[],
        )


class CreateProjectRequest(BaseModel):
    """Request to create a new project."""

    title: str = Field(..., min_length=1, max_length=200)
    description: str | None = Field(None, max_length=1000)
    owner: str | None = None


class AddProjectItemRequest(BaseModel):
    """Request to add an item to a project."""

    type: str = Field(..., min_length=1, max_length=50)
    content: str = Field(..., min_length=1, max_length=50000)
    user_question: str | None = Field(
        None, max_length=500, description="User's original question (used as title)"
    )
    title: str | None = Field(None, max_length=200)
    metadata: dict[str, Any] | None = None


class Graph(BaseModel):
    """Saved graph from chat."""

    id: str
    type: str
    content: str
    title: str
    query: str | None = None
    created_at: datetime | None = None
    metadata: dict[str, Any] | None = None
    user_id: str | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "Graph":
        return cls(
            id=str(row.get("id", "")),
            type=str(row.get("type", "")),
            content=row.get("content", ""),
            title=str(row.get("title", "")),
            query=row.get("query"),
            created_at=row.get("created_at"),
            metadata=_parse_json_field(row.get("metadata")),
            user_id=row.get("user_id"),
        )


class SaveGraphRequest(BaseModel):
    """Request to save a graph from chat."""

    type: str = Field(..., min_length=1, max_length=50)
    content: str = Field(..., min_length=1, max_length=50000)
    title: str = Field(..., min_length=1, max_length=200)
    query: str | None = None
    metadata: dict[str, Any] | None = None
    user_id: str | None = Field(None, max_length=100)


class DeleteGraphsRequest(BaseModel):
    """Request to delete multiple graphs."""

    graph_ids: list[str] = Field(..., min_length=1)


class InformeSummary(BaseModel):
    """Informe as shown in the list view."""

    id: str
    title: str
    description: str | None = None
    owner: str | None = None
    created_at: datetime | None = None
    graph_count: int = 0

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "InformeSummary":
        return cls(
            id=str(row["id"]),
            title=str(row["title"]),
            description=row.get("description"),
            owner=row.get("owner"),
            created_at=row.get("created_at"),
            graph_count=row.get("graph_count", 0),
        )


class InformeGraph(BaseModel):
    """A graph inside an informe."""

    item_id: str
    graph_id: str
    type: str
    content: str
    title: str
    query: str | None = None
    created_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "InformeGraph":
        return cls(
            item_id=str(row["item_id"]),
            graph_id=str(row["graph_id"]),
            type=str(row.get("type", "")),
            content=row.get("content", ""),
            title=str(row.get("title", "")),
            query=row.get("query"),
            created_at=row.get("created_at"),
        )


class InformeDetail(BaseModel):
    """Full informe with its graphs."""

    id: str
    title: str
    description: str | None = None
    owner: str | None = None
    created_at: datetime | None = None
    graphs: list[InformeGraph] = []


class CreateInformeRequest(BaseModel):
    """Request to create a new informe."""

    title: str = Field(..., min_length=1, max_length=200)
    description: str | None = Field(None, max_length=1000)
    owner: str | None = Field(None, max_length=100)


class AddGraphsToInformeRequest(BaseModel):
    """Request to add one or more graphs to an informe."""

    graph_ids: list[str] = Field(..., min_length=1)


class DeleteInformesRequest(BaseModel):
    """Request to delete multiple informes."""

    informe_ids: list[str] = Field(..., min_length=1)


class OperationResponse(BaseModel):
    """Standard response for single-resource mutations."""

    status: str = "success"
    id: str
    message: str | None = None


class BulkOperationResponse(BaseModel):
    """Standard response for bulk mutations."""

    status: str = "success"
    deleted_count: int
