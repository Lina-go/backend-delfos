"""Health check endpoint."""

from fastapi import APIRouter

from src.api.models import HealthResponse
from src.config.settings import get_settings

router = APIRouter()

settings = get_settings()


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Health check."""
    return HealthResponse(status="healthy", version=settings.app_version)
