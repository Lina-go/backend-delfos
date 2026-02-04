"""Request/Response models for API endpoints."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    """Request model for chat endpoint."""

    message: str = Field(..., description="User's natural language question")
    user_id: str = Field(..., description="User identifier")


class ChatResponse(BaseModel):
    """Response model for chat endpoint."""

    patron: str = Field(..., description="Analytical pattern")
    datos: list[dict[str, Any]] = Field(..., description="SQL query results")
    arquetipo: str = Field(..., description="Pattern type (A-N)")
    visualizacion: str = Field(..., description="YES or NO")
    tipo_grafica: str | None = Field(None, description="Chart type")
    imagen: str | None = Field(None, description="Chart image URL")
    html_url: str | None = Field(None, description="Chart HTML URL")
    link_power_bi: str | None = Field(None, description="Power BI URL")
    insight: str | None = Field(None, description="Generated insight")
    error: str | None = Field(
        "", description="Error message if error occurred, empty string otherwise"
    )


class HealthResponse(BaseModel):
    """Response model for health endpoint."""

    status: str = Field(..., description="Service status")
    version: str = Field(..., description="Application version")


class SchemaResponse(BaseModel):
    """Response model for schema endpoint."""

    tables: list[dict[str, Any]] = Field(..., description="List of tables with schema information")


class ProjectItem(BaseModel):
    """Item stored in a project."""

    id: str
    type: str
    content: str
    title: str | None = None
    created_at: datetime | None = None
    metadata: dict[str, Any] | None = {}


class Project(BaseModel):
    """Project definition."""

    id: str
    title: str
    description: str | None = None
    owner: str | None = None
    created_at: datetime | None = None
    items: list[ProjectItem] = []


class CreateProjectRequest(BaseModel):
    """Request to create a new project."""

    title: str
    description: str | None = ""
    owner: str = "Andres Leon"


class AddProjectItemRequest(BaseModel):
    """Request to add an item to a project."""

    type: str
    content: str
    user_question: str | None = Field(None, description="User's original question (used as title)")
    title: str | None = None  # Fallback if user_question is not provided
    metadata: dict[str, Any] | None = {}


class Graph(BaseModel):
    """Saved graph from chat."""

    id: str
    type: str
    content: str
    title: str
    query: str | None = None
    created_at: datetime | None = None
    metadata: dict[str, Any] | None = {}
    user_id: str | None = None


class SaveGraphRequest(BaseModel):
    """Request to save a graph from chat."""

    type: str
    content: str
    title: str
    query: str | None = None
    metadata: dict[str, Any] | None = {}
    user_id: str | None = None

class DeleteGraphsRequest(BaseModel):
    """Request to delete multiple graphs."""

    graph_ids: list[str]

class InformeSummary(BaseModel):
    """Informe as shown in the list view."""

    id: str
    title: str
    description: str | None = None
    owner: str | None = None
    created_at: datetime | None = None
    graph_count: int = 0


class InformeGraph(BaseModel):
    """A graph inside an informe."""

    item_id: str
    graph_id: str
    type: str
    content: str
    title: str
    query: str | None = None
    created_at: datetime | None = None


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

    title: str
    description: str | None = ""
    owner: str | None = None


class AddGraphsToInformeRequest(BaseModel):
    """Request to add one or more graphs to an informe."""

    graph_ids: list[str]