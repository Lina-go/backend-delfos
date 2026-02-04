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


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_get_graphs_empty(mock_query, client):
    """Return empty list when no graphs exist."""
    mock_query.return_value = []
    response = client.get("/api/graphs")
    assert response.status_code == 200
    assert response.json() == []


@patch("src.api.router.execute_query", new_callable=AsyncMock)
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


@patch("src.api.router.execute_query", new_callable=AsyncMock)
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


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
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
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "id" in data


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_save_graph_cleans_sas_token(mock_insert, client):
    """Strip SAS token from blob URL before saving."""
    mock_insert.return_value = {"success": True}
    blob_url = "https://mystorage.blob.core.windows.net/charts/chart.html?sv=2021&sig=abc"
    response = client.post("/api/graphs", json={
        "type": "PIE",
        "content": blob_url,
        "title": "Test",
    })
    assert response.status_code == 200
    insert_params = mock_insert.call_args[0][2]
    saved_url = insert_params[2]  # content is 3rd param
    assert "?sv=" not in saved_url
    assert "blob.core.windows.net" in saved_url


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
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


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_delete_graph_success(mock_insert, client):
    """Delete a single graph."""
    mock_insert.return_value = {"success": True}
    response = client.delete("/api/graphs/abc-123")
    assert response.status_code == 200
    assert response.json()["id"] == "abc-123"


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_delete_graph_db_error(mock_insert, client):
    """Return 500 on database error."""
    mock_insert.return_value = {"success": False, "error": "Not found"}
    response = client.delete("/api/graphs/abc-123")
    assert response.status_code == 500


# ==========================================
#  DELETE /api/graphs (bulk)
# ==========================================


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_delete_graphs_bulk_success(mock_insert, client):
    """Delete multiple graphs at once."""
    mock_insert.return_value = {"success": True}
    response = client.request("DELETE", "/api/graphs", json={"graph_ids": ["id-1", "id-2", "id-3"]})
    assert response.status_code == 200
    assert response.json()["deleted_count"] == 3


def test_delete_graphs_bulk_empty(client):
    """Return 400 when no IDs provided."""
    response = client.request("DELETE", "/api/graphs", json={"graph_ids": []})
    assert response.status_code == 400

# ==========================================
#  PATCH /api/graphs/{graph_id}/refresh
# ==========================================


@patch("src.api.router.BlobStorageClient")
@patch("src.api.router.execute_insert", new_callable=AsyncMock)
@patch("src.api.router.PipelineOrchestrator")
@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_refresh_graph_success(mock_query, mock_orch_cls, mock_insert, mock_blob_cls, client):
    """Refresh a graph successfully."""
    # DB returns graph with query
    mock_query.return_value = [
        {"type": "line", "content": "https://old.blob.core.windows.net/charts/old.html", "title": "Test", "query": "SELECT 1", "user_id": "user1"}
    ]

    # Orchestrator returns new content
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.return_value = {
        "content": "https://mystorage.blob.core.windows.net/charts/new.html",
        "row_count": 5,
    }
    mock_orch_cls.return_value = mock_orch

    # DB update succeeds
    mock_insert.return_value = {"success": True}

    # Blob signing returns same URL (no real SAS in test)
    mock_storage = AsyncMock()
    mock_blob_cls.return_value = mock_storage

    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["id"] == "abc-123"
    assert data["row_count"] == 5

    # Verify orchestrator was called with correct params
    mock_orch.refresh_graph.assert_called_once_with(
        sql="SELECT 1", chart_type="line", title="Test", user_id="user1",
    )
    mock_orch.close.assert_called_once()


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_refresh_graph_not_found(mock_query, client):
    """Return 404 when graph doesn't exist."""
    mock_query.return_value = []
    response = client.patch("/api/graphs/nonexistent/refresh")
    assert response.status_code == 404


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_refresh_graph_no_query(mock_query, client):
    """Return 400 when graph has no stored SQL query."""
    mock_query.return_value = [
        {"type": "bar", "content": "https://example.com/chart.html", "title": "Test", "query": None, "user_id": "user1"}
    ]
    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 400
    assert "no stored query" in response.json()["detail"]


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
@patch("src.api.router.PipelineOrchestrator")
@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_refresh_graph_pipeline_error(mock_query, mock_orch_cls, mock_insert, client):
    """Return 500 when pipeline returns error."""
    mock_query.return_value = [
        {"type": "bar", "content": "https://example.com/c.html", "title": "T", "query": "SELECT 1", "user_id": "u1"}
    ]
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.return_value = {"error": "Query returned no results"}
    mock_orch_cls.return_value = mock_orch

    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 500
    assert "no results" in response.json()["detail"]
    mock_orch.close.assert_called_once()


@patch("src.api.router.PipelineOrchestrator")
@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_refresh_graph_orchestrator_exception(mock_query, mock_orch_cls, client):
    """Return 500 and cleanup on orchestrator exception."""
    mock_query.return_value = [
        {"type": "bar", "content": "https://example.com/c.html", "title": "T", "query": "SELECT 1", "user_id": "u1"}
    ]
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.side_effect = RuntimeError("Connection timeout")
    mock_orch_cls.return_value = mock_orch

    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 500
    mock_orch.close.assert_called_once()


@patch("src.api.router.BlobStorageClient")
@patch("src.api.router.execute_insert", new_callable=AsyncMock)
@patch("src.api.router.PipelineOrchestrator")
@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_refresh_graph_db_update_fails(mock_query, mock_orch_cls, mock_insert, mock_blob_cls, client):
    """Return 500 when DB update after refresh fails."""
    mock_query.return_value = [
        {"type": "bar", "content": "https://example.com/c.html", "title": "T", "query": "SELECT 1", "user_id": "u1"}
    ]
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.return_value = {"content": "https://mystorage.blob.core.windows.net/charts/new.html", "row_count": 3}
    mock_orch_cls.return_value = mock_orch

    mock_insert.return_value = {"success": False, "error": "Write conflict"}

    response = client.patch("/api/graphs/abc-123/refresh")
    assert response.status_code == 500
    assert "Write conflict" in response.json()["detail"]