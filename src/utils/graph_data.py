"""Shared graph data utilities used by advisor and bullet generation."""

import json
import logging
from decimal import Decimal
from typing import Any

from src.config.settings import Settings
from src.infrastructure.database.connection import execute_wh_query

logger = logging.getLogger(__name__)


def parse_graph_content(raw: str) -> Any:
    """Parse graph content from JSON string into a dict."""
    if not raw or not raw.strip():
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw


def make_json_safe(value: Any) -> Any:
    """Convert Decimal/date/datetime/bytes values to JSON-serializable types."""
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def truncate_data_points(data: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Keep only the last *limit* data_points to stay within token budget."""
    if len(data) <= limit:
        return data
    return data[-limit:]


async def fetch_graph_data(
    settings: Settings, query: str, graph_title: str = ""
) -> list[dict[str, Any]] | None:
    """Execute a graph's stored SQL query against the warehouse."""
    try:
        rows = await execute_wh_query(settings, query)
        return [{k: make_json_safe(v) for k, v in row.items()} for row in rows]
    except Exception as e:
        logger.warning(
            "Failed to fetch graph data for '%s': %s | SQL: %.200s",
            graph_title, e, query,
        )
        return None
