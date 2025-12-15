"""Tests for table selector."""

import pytest

from src.services.schema.table_selector import TableSelector


@pytest.mark.asyncio
async def test_table_selector(settings):
    """Test table selector."""
    selector = TableSelector(settings)
    tables = await selector.select_tables("How many customers?")
    assert isinstance(tables, list)
