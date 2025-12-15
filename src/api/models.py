"""Request/Response models for API endpoints."""

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
