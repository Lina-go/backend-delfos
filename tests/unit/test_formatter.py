"""Tests for response formatter."""

import pytest
from src.services.formatting.formatter import ResponseFormatter
from src.orchestrator.state import PipelineState


@pytest.mark.asyncio
async def test_response_formatter(settings):
    """Test response formatter."""
    formatter = ResponseFormatter(settings)
    state = PipelineState(
        user_message="Test question",
        user_id="test_user",
        sql_results=[{"col1": "value1"}],
    )
    result = await formatter.format(state)
    assert "patron" in result
    assert "datos" in result

