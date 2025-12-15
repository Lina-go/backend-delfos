"""Pytest configuration and fixtures."""

import pytest

from src.config.settings import Settings


@pytest.fixture
def settings():
    """Provide settings fixture."""
    return Settings()
