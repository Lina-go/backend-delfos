"""Request/Response models for API endpoints."""

from typing import Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime


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
    title: Optional[str] = None
    created_at: Optional[datetime] = None
    metadata: Optional[dict[str, Any]] = {}

class Project(BaseModel):
    """Project definition."""
    id: str
    title: str
    description: Optional[str] = None
    owner: Optional[str] = None
    created_at: Optional[datetime] = None
    items: list[ProjectItem] = []

class CreateProjectRequest(BaseModel):
    """Request to create a new project."""
    title: str
    description: Optional[str] = ""
    owner: str = "Andres Leon"

class AddProjectItemRequest(BaseModel):
    """Request to add an item to a project."""
    type: str
    content: str
    title: Optional[str] = "Nueva Gráfica"
    metadata: Optional[dict[str, Any]] = {}