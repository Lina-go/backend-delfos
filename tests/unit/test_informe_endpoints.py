"""Tests for informe endpoints."""

import pytest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from src.app import app


@pytest.fixture
def client():
    return TestClient(app)


# ==========================================
#  GET /api/informes
# ==========================================


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_list_informes_empty(mock_query, client):
    mock_query.return_value = []
    response = client.get("/api/informes")
    assert response.status_code == 200
    assert response.json() == []


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_list_informes_with_data(mock_query, client):
    mock_query.return_value = [
        {
            "id": "inf-1",
            "title": "Junta Directiva - Cierre 2025",
            "description": "",
            "owner": "Andres Leon",
            "created_at": None,
            "graph_count": 5,
        },
        {
            "id": "inf-2",
            "title": "Comité de Riesgos",
            "description": None,
            "owner": "Andres Leon",
            "created_at": None,
            "graph_count": 3,
        },
    ]
    response = client.get("/api/informes")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["title"] == "Junta Directiva - Cierre 2025"
    assert data[0]["graph_count"] == 5


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_list_informes_filter_by_owner(mock_query, client):
    mock_query.return_value = []
    response = client.get("/api/informes?owner=Andres Leon")
    assert response.status_code == 200
    sql_arg = mock_query.call_args[0][1]
    assert "owner = ?" in sql_arg


# ==========================================
#  POST /api/informes
# ==========================================


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_create_informe_success(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.post("/api/informes", json={
        "title": "Comité de Riesgos",
        "description": "Informe mensual",
        "owner": "Andres Leon",
    })
    assert response.status_code == 200
    data = response.json()
    assert data["title"] == "Comité de Riesgos"
    assert "id" in data
    assert data["graph_count"] == 0


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
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


@patch("src.api.router.BlobStorageClient")
@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_get_informe_detail(mock_query, mock_blob_class, client):
    mock_query.side_effect = [
        [{"id": "inf-1", "title": "Junta Directiva", "description": "", "owner": "Andres Leon", "created_at": None}],
        [
            {
                "item_id": "item-1",
                "graph_id": "g-1",
                "type": "PIE",
                "content": "https://example.com/chart.html",
                "title": "Market Share",
                "query": "SELECT ...",
                "created_at": None,
            }
        ],
    ]
    mock_blob = AsyncMock()
    mock_blob_class.return_value = mock_blob
    mock_blob.close = AsyncMock()

    response = client.get("/api/informes/inf-1")
    assert response.status_code == 200
    data = response.json()
    assert data["title"] == "Junta Directiva"
    assert len(data["graphs"]) == 1
    assert data["graphs"][0]["graph_id"] == "g-1"


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_get_informe_not_found(mock_query, client):
    mock_query.return_value = []
    response = client.get("/api/informes/nonexistent")
    assert response.status_code == 404


# ==========================================
#  DELETE /api/informes/{informe_id}
# ==========================================


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_delete_informe_success(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.delete("/api/informes/inf-1")
    assert response.status_code == 200
    assert response.json()["id"] == "inf-1"
    assert mock_insert.call_count == 2


# ==========================================
#  POST /api/informes/{id}/graphs
# ==========================================


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_add_graphs_to_informe(mock_query, mock_insert, client):
    mock_query.side_effect = [
        [{"id": "inf-1"}],
        [{"id": "g-1", "title": "Market Share"}, {"id": "g-2", "title": "ROE"}],
        [],
    ]
    mock_insert.return_value = {"success": True}

    response = client.post("/api/informes/inf-1/graphs", json={"graph_ids": ["g-1", "g-2"]})
    assert response.status_code == 200
    data = response.json()
    assert len(data["added"]) == 2
    assert data["skipped_duplicates"] == []


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_add_graphs_skips_duplicates(mock_query, mock_insert, client):
    mock_query.side_effect = [
        [{"id": "inf-1"}],
        [{"id": "g-1", "title": "Market Share"}],
        [{"graph_id": "g-1"}],
    ]
    mock_insert.return_value = {"success": True}

    response = client.post("/api/informes/inf-1/graphs", json={"graph_ids": ["g-1"]})
    assert response.status_code == 200
    assert response.json()["skipped_duplicates"] == ["g-1"]


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_add_graphs_informe_not_found(mock_query, client):
    mock_query.return_value = []
    response = client.post("/api/informes/nonexistent/graphs", json={"graph_ids": ["g-1"]})
    assert response.status_code == 404


def test_add_graphs_empty_list(client):
    response = client.post("/api/informes/inf-1/graphs", json={"graph_ids": []})
    assert response.status_code == 400


# ==========================================
#  DELETE /api/informes/{id}/graphs/{item_id}
# ==========================================


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_remove_graph_from_informe(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.delete("/api/informes/inf-1/graphs/item-1")
    assert response.status_code == 200
    assert response.json()["item_id"] == "item-1"


# ==========================================
#  PATCH /api/informes/{id}/refresh
# ==========================================


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_refresh_informe_no_graphs(mock_query, client):
    mock_query.return_value = []
    response = client.patch("/api/informes/inf-1/refresh")
    assert response.status_code == 404