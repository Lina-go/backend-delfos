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