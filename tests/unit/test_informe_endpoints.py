"""Tests for informe endpoints."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.app import app


@pytest.fixture
def client():
    return TestClient(app)


# ==========================================
#  GET /api/informes
# ==========================================


@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_list_informes_empty(mock_query, client):
    mock_query.return_value = []
    response = client.get("/api/informes")
    assert response.status_code == 200
    assert response.json() == []


@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_list_informes_with_data(mock_query, client):
    mock_query.return_value = [
        {"id": "inf-1", "title": "Junta Directiva - Cierre 2025", "description": "", "owner": "Andres Leon", "created_at": None, "graph_count": 5},
        {"id": "inf-2", "title": "Comité de Riesgos", "description": None, "owner": "Andres Leon", "created_at": None, "graph_count": 3},
    ]
    response = client.get("/api/informes")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["title"] == "Junta Directiva - Cierre 2025"
    assert data[0]["graph_count"] == 5


@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_list_informes_filter_by_owner(mock_query, client):
    mock_query.return_value = []
    response = client.get("/api/informes?owner=Andres Leon")
    assert response.status_code == 200
    assert "owner = ?" in mock_query.call_args[0][1]


# ==========================================
#  POST /api/informes
# ==========================================


@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
def test_create_informe_success(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.post("/api/informes", json={"title": "Comité de Riesgos", "description": "Informe mensual", "owner": "Andres Leon"})
    assert response.status_code == 201
    data = response.json()
    assert data["title"] == "Comité de Riesgos"
    assert data["graph_count"] == 0


@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
def test_create_informe_db_error(mock_insert, client):
    mock_insert.return_value = {"success": False, "error": "Connection failed"}
    response = client.post("/api/informes", json={"title": "Test"})
    assert response.status_code == 500


def test_create_informe_missing_title(client):
    response = client.post("/api/informes", json={})
    assert response.status_code == 422


# ==========================================
#  GET /api/informes/{informe_id}
# ==========================================


@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_get_informe_detail(mock_query, client):
    mock_query.side_effect = [
        [{"id": "inf-1", "title": "Junta Directiva", "description": "", "owner": "Andres Leon", "created_at": None}],
        [{"item_id": "item-1", "graph_id": "g-1", "type": "PIE", "content": '{"data_points": []}', "title": "Market Share", "query": "SELECT 1", "created_at": None}],
        [],  # labels query
    ]
    response = client.get("/api/informes/inf-1")
    assert response.status_code == 200
    assert response.json()["title"] == "Junta Directiva"
    assert response.json()["graphs"][0]["graph_id"] == "g-1"


@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_get_informe_not_found(mock_query, client):
    mock_query.return_value = []
    response = client.get("/api/informes/nonexistent")
    assert response.status_code == 404


# ==========================================
#  DELETE /api/informes/{informe_id}
# ==========================================


@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
def test_delete_informe_success(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.delete("/api/informes/inf-1")
    assert response.status_code == 200
    assert response.json()["id"] == "inf-1"
    assert mock_insert.call_count == 3  # items + labels + project


@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
def test_delete_informe_db_error(mock_insert, client):
    mock_insert.side_effect = [{"success": True}, {"success": False, "error": "DB error"}]
    response = client.delete("/api/informes/inf-1")
    assert response.status_code == 500


# ==========================================
#  POST /api/informes/{id}/graphs
# ==========================================


@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_add_graphs_to_informe(mock_query, mock_insert, client):
    mock_query.side_effect = [
        [{"id": "inf-1"}],
        [{"id": "g-1", "title": "Market Share"}, {"id": "g-2", "title": "ROE"}],
        [],
    ]
    mock_insert.return_value = {"success": True}
    response = client.post("/api/informes/inf-1/graphs", json={"graph_ids": ["g-1", "g-2"]})
    assert response.status_code == 201
    assert len(response.json()["added"]) == 2


@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_add_graphs_skips_duplicates(mock_query, mock_insert, client):
    mock_query.side_effect = [[{"id": "inf-1"}], [{"id": "g-1", "title": "Market Share"}], [{"graph_id": "g-1"}]]
    mock_insert.return_value = {"success": True}
    response = client.post("/api/informes/inf-1/graphs", json={"graph_ids": ["g-1"]})
    assert response.json()["skipped_duplicates"] == ["g-1"]


@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_add_graphs_not_found_in_db(mock_query, mock_insert, client):
    mock_query.side_effect = [[{"id": "inf-1"}], [], []]
    response = client.post("/api/informes/inf-1/graphs", json={"graph_ids": ["nonexistent"]})
    assert response.json()["not_found"] == ["nonexistent"]


@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_add_graphs_informe_not_found(mock_query, client):
    mock_query.return_value = []
    response = client.post("/api/informes/nonexistent/graphs", json={"graph_ids": ["g-1"]})
    assert response.status_code == 404


def test_add_graphs_empty_list(client):
    """Return 422 when no IDs provided (Pydantic min_length=1 validation)."""
    response = client.post("/api/informes/inf-1/graphs", json={"graph_ids": []})
    assert response.status_code == 422


# ==========================================
#  DELETE /api/informes/{id}/graphs/{item_id}
# ==========================================


@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
def test_remove_graph_from_informe(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.delete("/api/informes/inf-1/graphs/item-1")
    assert response.status_code == 200
    assert response.json()["id"] == "item-1"


@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
def test_remove_graph_db_error(mock_insert, client):
    mock_insert.return_value = {"success": False, "error": "DB error"}
    response = client.delete("/api/informes/inf-1/graphs/item-1")
    assert response.status_code == 500


# ==========================================
#  PATCH /api/informes/{id}/refresh
# ==========================================


@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_refresh_informe_no_graphs(mock_query, client):
    mock_query.return_value = []
    response = client.patch("/api/informes/inf-1/refresh")
    assert response.status_code == 404


@patch("src.services.informes.service.PipelineOrchestrator")
@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_refresh_informe_success(mock_query, mock_insert, mock_orch_class, client):
    mock_query.side_effect = [
        [{"graph_id": "g-1"}],
        [{"type": "BAR", "title": "Test", "query": "SELECT 1", "user_id": "user1"}],
    ]
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.return_value = {"data_points": [{"x_value": "2024", "y_value": 100}], "metric_name": "ventas"}
    mock_orch_class.return_value = mock_orch
    mock_insert.return_value = {"success": True}

    response = client.patch("/api/informes/inf-1/refresh")
    assert response.status_code == 200
    assert "g-1" in response.json()["refreshed"]


@patch("src.services.informes.service.PipelineOrchestrator")
@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_refresh_informe_no_query(mock_query, mock_orch_class, client):
    mock_query.side_effect = [
        [{"graph_id": "g-1"}],
        [{"type": "BAR", "title": "Test", "query": None, "user_id": "user1"}],
    ]
    mock_orch_class.return_value = AsyncMock()
    response = client.patch("/api/informes/inf-1/refresh")
    assert response.status_code == 200
    assert len(response.json()["skipped"]) == 1


@patch("src.services.informes.service.PipelineOrchestrator")
@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_refresh_informe_graph_not_found(mock_query, mock_orch_class, client):
    mock_query.side_effect = [[{"graph_id": "g-1"}], []]
    mock_orch_class.return_value = AsyncMock()
    response = client.patch("/api/informes/inf-1/refresh")
    assert response.status_code == 200
    assert len(response.json()["failed"]) == 1


@patch("src.services.informes.service.PipelineOrchestrator")
@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_refresh_informe_refresh_error(mock_query, mock_orch_class, client):
    mock_query.side_effect = [
        [{"graph_id": "g-1"}],
        [{"type": "BAR", "title": "Test", "query": "SELECT 1", "user_id": "user1"}],
    ]
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.return_value = {"error": "SQL failed"}
    mock_orch_class.return_value = mock_orch
    response = client.patch("/api/informes/inf-1/refresh")
    assert response.status_code == 200
    assert len(response.json()["failed"]) == 1


@patch("src.services.informes.service.PipelineOrchestrator")
@patch("src.services.informes.service.execute_query", new_callable=AsyncMock)
def test_refresh_informe_exception(mock_query, mock_orch_class, client):
    mock_query.side_effect = [
        [{"graph_id": "g-1"}],
        [{"type": "BAR", "title": "Test", "query": "SELECT 1", "user_id": "user1"}],
    ]
    mock_orch = AsyncMock()
    mock_orch.refresh_graph.side_effect = Exception("Connection lost")
    mock_orch_class.return_value = mock_orch
    response = client.patch("/api/informes/inf-1/refresh")
    assert response.status_code == 200
    assert len(response.json()["failed"]) == 1

@patch("src.services.informes.service.execute_insert", new_callable=AsyncMock)
def test_delete_informes_bulk(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.request("DELETE", "/api/informes", json={"informe_ids": ["inf-1", "inf-2"]})
    assert response.status_code == 200
    assert response.json()["deleted_count"] == 2


def test_delete_informes_bulk_empty(client):
    """Return 422 when no IDs provided (Pydantic min_length=1 validation)."""
    response = client.request("DELETE", "/api/informes", json={"informe_ids": []})
    assert response.status_code == 422
