"""Tests for existing router endpoints (health, cache, chat, projects, helpers)."""

from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.app import app
from src.api.router import _clean_blob_url, _parse_metadata


@pytest.fixture
def client():
    return TestClient(app)


# ==========================================
#  HELPER FUNCTIONS
# ==========================================


def test_clean_blob_url_empty():
    assert _clean_blob_url("") == ""
    assert _clean_blob_url(None) == None


def test_clean_blob_url_non_blob():
    assert _clean_blob_url("https://example.com/file.html") == "https://example.com/file.html"


def test_clean_blob_url_strips_sas():
    url = "https://store.blob.core.windows.net/charts/file.html?sv=2023&sig=abc"
    result = _clean_blob_url(url)
    assert "blob.core.windows.net" in result
    assert "sig=abc" not in result


def test_parse_metadata_none():
    assert _parse_metadata(None) == {}
    assert _parse_metadata("") == {}


def test_parse_metadata_valid_json():
    assert _parse_metadata('{"key": "value"}') == {"key": "value"}


def test_parse_metadata_dict():
    assert _parse_metadata({"key": "value"}) == {"key": "value"}


def test_parse_metadata_invalid_json():
    assert _parse_metadata("{invalid}") == {}


# ==========================================
#  HEALTH & CACHE
# ==========================================


def test_health(client):
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


@patch("src.api.router.SemanticCache")
def test_cache_stats(mock_cache, client):
    mock_cache.get_stats.return_value = {"hits": 0, "misses": 0}
    response = client.get("/api/cache/stats")
    assert response.status_code == 200


@patch("src.api.router.SemanticCache")
def test_cache_clear(mock_cache, client):
    response = client.delete("/api/cache")
    assert response.status_code == 200
    assert response.json()["status"] == "success"


# ==========================================
#  CHAT ENDPOINTS
# ==========================================


@patch("src.api.router.PipelineOrchestrator")
def test_chat_success(mock_orch_class, client):
    mock_orch = AsyncMock()
    mock_orch.process.return_value = {
        "response": "ok",
        "sql": "SELECT 1",
        "patron": "COMPARACION",
        "datos": [],
        "arquetipo": "A",
        "visualizacion": "bar",
    }
    mock_orch_class.return_value = mock_orch
    response = client.post("/api/chat", json={"message": "hola", "user_id": "u1"})
    assert response.status_code == 200


@patch("src.api.router.PipelineOrchestrator")
def test_chat_error(mock_orch_class, client):
    mock_orch = AsyncMock()
    mock_orch.process.side_effect = Exception("Pipeline failed")
    mock_orch_class.return_value = mock_orch
    response = client.post("/api/chat", json={"message": "hola", "user_id": "u1"})
    assert response.status_code == 500


@patch("src.api.router.PipelineOrchestrator")
def test_chat_stream(mock_orch_class, client):
    async def fake_stream(*args):
        yield {"step": "done", "data": "ok"}

    mock_orch = AsyncMock()
    mock_orch.process_stream = fake_stream
    mock_orch_class.return_value = mock_orch
    response = client.post("/api/chat/stream", json={"message": "hola", "user_id": "u1"})
    assert response.status_code == 200
    assert "data:" in response.text


@patch("src.api.router.PipelineOrchestrator")
def test_chat_stream_error(mock_orch_class, client):
    async def fail_stream(*args):
        raise Exception("Stream failed")
        yield  # noqa: unreachable

    mock_orch = AsyncMock()
    mock_orch.process_stream = fail_stream
    mock_orch_class.return_value = mock_orch
    response = client.post("/api/chat/stream", json={"message": "hola", "user_id": "u1"})
    assert response.status_code == 200
    assert "error" in response.text


# ==========================================
#  PROJECT ENDPOINTS
# ==========================================


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_get_projects(mock_query, client):
    mock_query.return_value = [
        {"id": "p-1", "title": "Test", "description": None, "owner": "user", "created_at": None}
    ]
    response = client.get("/api/projects")
    assert response.status_code == 200
    assert len(response.json()) == 1


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_get_projects_error(mock_query, client):
    mock_query.side_effect = Exception("DB error")
    response = client.get("/api/projects")
    assert response.status_code == 200
    assert response.json() == []


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_create_project(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.post("/api/projects", json={"title": "Test", "description": "d", "owner": "u"})
    assert response.status_code == 200
    assert response.json()["title"] == "Test"


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_create_project_error(mock_insert, client):
    mock_insert.return_value = {"success": False, "error": "DB error"}
    response = client.post("/api/projects", json={"title": "Test"})
    assert response.status_code == 500


@patch("src.api.router.execute_insert", new_callable=AsyncMock)
def test_add_project_item(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.post("/api/projects/p-1/items", json={
        "type": "chart",
        "content": "https://store.blob.core.windows.net/charts/file.html?sig=abc",
        "title": "Test",
    })
    assert response.status_code == 200
    assert response.json()["status"] == "success"


@patch("src.api.router.BlobStorageClient")
@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_get_project_items(mock_query, mock_blob_class, client):
    mock_query.return_value = [
        {"id": "i-1", "projectId": "p-1", "type": "chart", "content": "https://example.com/f.html", "title": "T", "created_at": None}
    ]
    mock_blob = AsyncMock()
    mock_blob_class.return_value = mock_blob
    response = client.get("/api/projects/p-1/items")
    assert response.status_code == 200
    assert len(response.json()) == 1


@patch("src.api.router.execute_query", new_callable=AsyncMock)
def test_get_project_items_error(mock_query, client):
    mock_query.side_effect = Exception("DB error")
    response = client.get("/api/projects/p-1/items")
    assert response.status_code == 200
    assert response.json() == []