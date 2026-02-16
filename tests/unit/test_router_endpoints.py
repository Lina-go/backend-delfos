"""Tests for router endpoints (health, cache, chat, projects, helpers)."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.models import Graph
from src.app import app


@pytest.fixture
def client():
    return TestClient(app)


# ==========================================
#  Graph.from_db_row metadata parsing
# ==========================================


def test_parse_metadata_none():
    row = {"id": "1", "type": "bar", "content": "", "title": "T", "metadata": None}
    graph = Graph.from_db_row(row)
    assert graph.metadata == {}


def test_parse_metadata_empty_string():
    row = {"id": "1", "type": "bar", "content": "", "title": "T", "metadata": ""}
    graph = Graph.from_db_row(row)
    assert graph.metadata == {}


def test_parse_metadata_valid_json():
    row = {"id": "1", "type": "bar", "content": "", "title": "T", "metadata": '{"key": "value"}'}
    graph = Graph.from_db_row(row)
    assert graph.metadata == {"key": "value"}


def test_parse_metadata_dict():
    row = {"id": "1", "type": "bar", "content": "", "title": "T", "metadata": {"key": "value"}}
    graph = Graph.from_db_row(row)
    assert graph.metadata == {"key": "value"}


def test_parse_metadata_invalid_json():
    row = {"id": "1", "type": "bar", "content": "", "title": "T", "metadata": "{invalid}"}
    graph = Graph.from_db_row(row)
    assert graph.metadata == {}


# ==========================================
#  HEALTH & CACHE
# ==========================================


def test_health(client):
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


@patch("src.api.routers.cache.SemanticCache")
def test_cache_stats(mock_cache, client):
    mock_cache.get_stats.return_value = {"hits": 0, "misses": 0}
    response = client.get("/api/cache/stats")
    assert response.status_code == 200


@patch("src.api.routers.cache.SemanticCache")
def test_cache_clear(mock_cache, client):
    response = client.delete("/api/cache")
    assert response.status_code == 200
    assert response.json()["status"] == "success"


# ==========================================
#  CHAT ENDPOINTS
# ==========================================


@patch("src.api.routers.chat.PipelineOrchestrator")
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
    mock_orch.__aenter__ = AsyncMock(return_value=mock_orch)
    mock_orch.__aexit__ = AsyncMock(return_value=False)
    mock_orch_class.return_value = mock_orch
    response = client.post("/api/chat", json={"message": "hola", "user_id": "u1"})
    assert response.status_code == 200


@patch("src.api.routers.chat.PipelineOrchestrator")
def test_chat_error(mock_orch_class, client):
    mock_orch = AsyncMock()
    mock_orch.process.side_effect = Exception("Pipeline failed")
    mock_orch.__aenter__ = AsyncMock(return_value=mock_orch)
    mock_orch.__aexit__ = AsyncMock(return_value=False)
    mock_orch_class.return_value = mock_orch
    response = client.post("/api/chat", json={"message": "hola", "user_id": "u1"})
    assert response.status_code == 500


@patch("src.api.routers.chat.PipelineOrchestrator")
def test_chat_stream(mock_orch_class, client):
    async def fake_stream(*args):
        yield {"step": "done", "data": "ok"}

    mock_orch = AsyncMock()
    mock_orch.process_stream = fake_stream
    mock_orch.__aenter__ = AsyncMock(return_value=mock_orch)
    mock_orch.__aexit__ = AsyncMock(return_value=False)
    mock_orch_class.return_value = mock_orch
    response = client.post("/api/chat/stream", json={"message": "hola", "user_id": "u1"})
    assert response.status_code == 200
    assert "data:" in response.text


@patch("src.api.routers.chat.PipelineOrchestrator")
def test_chat_stream_error(mock_orch_class, client):
    async def fail_stream(*args):
        raise Exception("Stream failed")
        yield  # noqa: unreachable

    mock_orch = AsyncMock()
    mock_orch.process_stream = fail_stream
    mock_orch.__aenter__ = AsyncMock(return_value=mock_orch)
    mock_orch.__aexit__ = AsyncMock(return_value=False)
    mock_orch_class.return_value = mock_orch
    response = client.post("/api/chat/stream", json={"message": "hola", "user_id": "u1"})
    assert response.status_code == 200
    assert "error" in response.text


# ==========================================
#  PROJECT ENDPOINTS
# ==========================================


@patch("src.services.projects.service.execute_query", new_callable=AsyncMock)
def test_get_projects(mock_query, client):
    mock_query.return_value = [
        {"id": "p-1", "title": "Test", "description": None, "owner": "user", "created_at": None}
    ]
    response = client.get("/api/projects")
    assert response.status_code == 200
    assert len(response.json()) == 1


@patch("src.services.projects.service.execute_query", new_callable=AsyncMock)
def test_get_projects_error(mock_query, client):
    mock_query.side_effect = Exception("DB error")
    response = client.get("/api/projects")
    assert response.status_code == 200
    assert response.json() == []


@patch("src.services.projects.service.execute_insert", new_callable=AsyncMock)
def test_create_project(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.post("/api/projects", json={"title": "Test", "description": "d", "owner": "u"})
    assert response.status_code == 201
    assert response.json()["title"] == "Test"


@patch("src.services.projects.service.execute_insert", new_callable=AsyncMock)
def test_create_project_error(mock_insert, client):
    mock_insert.return_value = {"success": False, "error": "DB error"}
    response = client.post("/api/projects", json={"title": "Test"})
    assert response.status_code == 500


@patch("src.services.projects.service.execute_insert", new_callable=AsyncMock)
def test_add_project_item(mock_insert, client):
    mock_insert.return_value = {"success": True}
    response = client.post("/api/projects/p-1/items", json={
        "type": "chart",
        "content": "https://store.blob.core.windows.net/charts/file.html?sig=abc",
        "title": "Test",
    })
    assert response.status_code == 201


@patch("src.services.projects.service.execute_query", new_callable=AsyncMock)
def test_get_project_items(mock_query, client):
    mock_query.return_value = [
        {"id": "i-1", "projectId": "p-1", "type": "chart", "content": '{"data_points": []}', "title": "T", "created_at": None}
    ]
    response = client.get("/api/projects/p-1/items")
    assert response.status_code == 200
    assert len(response.json()) == 1


@patch("src.services.projects.service.execute_query", new_callable=AsyncMock)
def test_get_project_items_error(mock_query, client):
    mock_query.side_effect = Exception("DB error")
    response = client.get("/api/projects/p-1/items")
    assert response.status_code == 200
    assert response.json() == []
