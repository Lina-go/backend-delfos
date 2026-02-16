"""Tests for graph endpoints."""

import pytest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from src.app import app


@pytest.fixture
def client():
    """Provide a FastAPI test client."""
    return TestClient(app)


# ==========================================
#  GET /api/graphs
# ==========================================


@patch("src.services.graphs.service.execute_query", new_callable=AsyncMock)
def test_get_graphs_empty(mock_query, client):
    """Return empty list when no graphs exist."""
    mock_query.return_value = []
    response = client.get("/api/graphs")
    assert response.status_code == 200
    assert response.json() == []


@patch("src.services.graphs.service.execute_query", new_callable=AsyncMock)
def test_get_graphs_with_data(mock_query, client):
    """Return graphs list."""
    mock_query.return_value = [
        {
            "id": "abc-123",
            "type": "BAR",
            "content": "https://example.com/chart.html",
            "title": "Test graph",
            "query": "SELECT 1",
            "created_at": None,
            "metadata": None,
            "user_id": "user1",
        }
    ]
    response = client.get("/api/graphs")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["id"] == "abc-123"
    assert data[0]["type"] == "BAR"


@patch("src.services.graphs.service.execute_query", new_callable=AsyncMock)
def test_get_graphs_filter_by_user(mock_query, client):
    """Filter graphs by user_id query param."""
    mock_query.return_value = []
    response = client.get("/api/graphs?user_id=user1")
    assert response.status_code == 200
    call_args = mock_query.call_args
    assert "WHERE user_id = ?" in call_args[0][1]


# ==========================================
#  POST /api/graphs
# ==========================================


@patch("src.services.graphs.service.execute_insert", new_callable=AsyncMock)
def test_save_graph_success(mock_insert, client):
    """Save a graph successfully."""
    mock_insert.return_value = {"success": True}
    response = client.post("/api/graphs", json={
        "type": "LINE",
        "content": "https://example.com/chart.html",
        "title": "Evolución cartera",
        "query": "SELECT * FROM dbo.Graphs",
        "user_id": "user1",
    })
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "success"
    assert "id" in data


@patch("src.services.graphs.service.execute_insert", new_callable=AsyncMock)
def test_save_graph_stores_content_directly(mock_insert, client):
    """Content is stored directly without transformation."""
    mock_insert.return_value = {"success": True}
    content = '{"data_points": [{"x_value": "2024", "y_value": 100}], "metric_name": "ventas"}'
    response = client.post("/api/graphs", json={
        "type": "PIE",
        "content": content,
        "title": "Test",
    })
    assert response.status_code == 201
    insert_params = mock_insert.call_args[0][2]
    saved_content = insert_params[2]  # content is 3rd param
    assert saved_content == content


@patch("src.services.graphs.service.execute_insert", new_callable=AsyncMock)
def test_save_graph_db_error(mock_insert, client):
    """Return 500 on database error."""
    mock_insert.return_value = {"success": False, "error": "Connection failed"}
    response = client.post("/api/graphs", json={
        "type": "BAR",
        "content": "https://example.com/chart.html",
        "title": "Test",
    })
    assert response.status_code == 500


def test_save_graph_missing_fields(client):
    """Return 422 on missing required fields."""
    response = client.post("/api/graphs", json={"type": "BAR"})
    assert response.status_code == 422


# ==========================================
#  DELETE /api/graphs/{graph_id}
# ==========================================


@patch("src.services.graphs.service.execute_insert", new_callable=AsyncMock)
def test_delete_graph_success(mock_insert, client):
    """Delete a single graph."""
    mock_insert.return_value = {"success": True}
    response = client.delete("/api/graphs/abc-123")
    assert response.status_code == 200
    assert response.json()["id"] == "abc-123"


@patch("src.services.graphs.service.execute_insert", new_callable=AsyncMock)
def test_delete_graph_db_error(mock_insert, client):
    """Return 500 on database error."""
    mock_insert.return_value = {"success": False, "error": "Not found"}
    response = client.delete("/api/graphs/abc-123")
    assert response.status_code == 500


# ==========================================
#  DELETE /api/graphs (bulk)
# ==========================================


@patch("src.services.graphs.service.execute_insert", new_callable=AsyncMock)
def test_delete_graphs_bulk_success(mock_insert, client):
    """Delete multiple graphs at once."""
    mock_insert.return_value = {"success": True}
    response = client.request("DELETE", "/api/graphs", json={"graph_ids": ["id-1", "id-2", "id-3"]})
    assert response.status_code == 200
    assert response.json()["deleted_count"] == 3


def test_delete_graphs_bulk_empty(client):
    """Return 422 when no IDs provided (Pydantic min_length=1 validation)."""
    response = client.request("DELETE", "/api/graphs", json={"graph_ids": []})
    assert response.status_code == 422

# ==========================================
#  PATCH /api/graphs/{graph_id}/refresh
# ==========================================


@patch("src.services.graphs.service.execute_insert", new_callable=AsyncMock)
@patch("src.services.graphs.service.PipelineOrchestrator")
@patch("src.services.graphs.service.execute_query", new_callable=AsyncMock)
def test_refresh_graph_success(mock_query, mock_orch_cls, mock_insert, client):
    """Refresh a graph successfully."""
    # First call: fetch graph; second call: re-fetch after update
    mock_query.side_effect = [
        [{"type": "line", "content": '{"data_points": []}', "title": "Test", "query": "SELECT 1", "user_id": "user1"}],
        [{"id": "abc-123", "type": "line", "content": '{"data_points": [{"x_value": "2024", "y_value": 100}], "metric_name": "ventas"}', "title": "Test", "query": "SELECT 1", "created_at": None, "metadata": None, "user_id": "user1"}],
    ]

    # Orchestrator returns data_points (context manager mock)
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.return_value = {
        "data_points": [{"x_value": "2024", "y_value": 100}],
        "metric_name": "ventas",
        "row_count": 5,
    }
    mock_orch.__aenter__ = AsyncMock(return_value=mock_orch)
    mock_orch.__aexit__ = AsyncMock(return_value=False)
    mock_orch_cls.return_value = mock_orch

    # DB update succeeds
    mock_insert.return_value = {"success": True}

    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "abc-123"

    # Verify orchestrator was called with correct params
    mock_orch.refresh_graph.assert_called_once_with(
        sql="SELECT 1", chart_type="line", title="Test", user_id="user1",
    )


@patch("src.services.graphs.service.execute_query", new_callable=AsyncMock)
def test_refresh_graph_not_found(mock_query, client):
    """Return 404 when graph doesn't exist."""
    mock_query.return_value = []
    response = client.patch("/api/graphs/nonexistent/refresh")
    assert response.status_code == 404


@patch("src.services.graphs.service.execute_query", new_callable=AsyncMock)
def test_refresh_graph_no_query(mock_query, client):
    """Return 400 when graph has no stored SQL query."""
    mock_query.return_value = [
        {"type": "bar", "content": "https://example.com/chart.html", "title": "Test", "query": None, "user_id": "user1"}
    ]
    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 400
    assert "no stored query" in response.json()["detail"]


@patch("src.services.graphs.service.execute_insert", new_callable=AsyncMock)
@patch("src.services.graphs.service.PipelineOrchestrator")
@patch("src.services.graphs.service.execute_query", new_callable=AsyncMock)
def test_refresh_graph_pipeline_error(mock_query, mock_orch_cls, mock_insert, client):
    """Return 500 when pipeline returns error."""
    mock_query.return_value = [
        {"type": "bar", "content": "https://example.com/c.html", "title": "T", "query": "SELECT 1", "user_id": "u1"}
    ]
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.return_value = {"error": "Query returned no results"}
    mock_orch.__aenter__ = AsyncMock(return_value=mock_orch)
    mock_orch.__aexit__ = AsyncMock(return_value=False)
    mock_orch_cls.return_value = mock_orch

    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to refresh graph"


@patch("src.services.graphs.service.PipelineOrchestrator")
@patch("src.services.graphs.service.execute_query", new_callable=AsyncMock)
def test_refresh_graph_orchestrator_exception(mock_query, mock_orch_cls, client):
    """Return 500 and cleanup on orchestrator exception."""
    mock_query.return_value = [
        {"type": "bar", "content": "https://example.com/c.html", "title": "T", "query": "SELECT 1", "user_id": "u1"}
    ]
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.side_effect = RuntimeError("Connection timeout")
    mock_orch.__aenter__ = AsyncMock(return_value=mock_orch)
    mock_orch.__aexit__ = AsyncMock(return_value=False)
    mock_orch_cls.return_value = mock_orch

    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 500


@patch("src.services.graphs.service.execute_insert", new_callable=AsyncMock)
@patch("src.services.graphs.service.PipelineOrchestrator")
@patch("src.services.graphs.service.execute_query", new_callable=AsyncMock)
def test_refresh_graph_db_update_fails(mock_query, mock_orch_cls, mock_insert, client):
    """Return 500 when DB update after refresh fails."""
    mock_query.return_value = [
        {"type": "bar", "content": '{"data_points": []}', "title": "T", "query": "SELECT 1", "user_id": "u1"}
    ]
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.return_value = {"data_points": [{"x_value": "2024", "y_value": 100}], "metric_name": "ventas", "row_count": 3}
    mock_orch.__aenter__ = AsyncMock(return_value=mock_orch)
    mock_orch.__aexit__ = AsyncMock(return_value=False)
    mock_orch_cls.return_value = mock_orch

    mock_insert.return_value = {"success": False, "error": "Write conflict"}

    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to update graph"
