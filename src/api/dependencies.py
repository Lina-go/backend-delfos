"""FastAPI dependencies."""

from functools import lru_cache

from src.config.settings import Settings, get_settings


@lru_cache
def get_settings_dependency() -> Settings:
    """Get settings as a FastAPI dependency."""
    return get_settings()
