"""Project persistence service."""

import logging
import uuid
from typing import Any

from src.api.models import AddProjectItemRequest, CreateProjectRequest, Project
from src.config.settings import Settings
from src.infrastructure.database.connection import execute_insert, execute_query
from src.infrastructure.database.helpers import audit_log, check_db_result

logger = logging.getLogger(__name__)


class ProjectService:
    """Handles project CRUD operations."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def list_projects(self, offset: int, limit: int) -> list[Project]:
        """List all projects."""
        sql = "SELECT id, title, description, owner, createdAt as created_at FROM dbo.Projects ORDER BY createdAt DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        try:
            projects_data = await execute_query(self.settings, sql, (offset, limit))
            return [Project.from_db_row(p) for p in projects_data]
        except Exception as e:
            logger.error("Error fetching projects: %s", e)
            return []

    async def create_project(self, request: CreateProjectRequest) -> Project:
        """Create a new project."""
        new_id = str(uuid.uuid4())
        sql = """
        INSERT INTO dbo.Projects (id, title, description, owner, createdAt)
        VALUES (?, ?, ?, ?, GETDATE())
        """
        result = await execute_insert(
            self.settings, sql, (new_id, request.title, request.description, request.owner)
        )
        check_db_result(result, "create project")
        audit_log("CREATE", "project", new_id)

        return Project(
            id=new_id,
            title=request.title,
            description=request.description,
            owner=request.owner,
            items=[],
        )

    async def add_item(self, project_id: str, request: AddProjectItemRequest) -> str:
        """Add a graph to a project. Returns the new item_id."""
        item_id = str(uuid.uuid4())
        MAX_TITLE_LENGTH = 200

        title = (request.user_question or request.title or "Nueva Gráfica").strip()
        if len(title) > MAX_TITLE_LENGTH:
            title = title[: MAX_TITLE_LENGTH - 3] + "..."

        sql = """
        INSERT INTO dbo.ProjectItems (id, projectId, type, content, title, createdAt)
        VALUES (?, ?, ?, ?, ?, GETDATE())
        """
        result = await execute_insert(
            self.settings,
            sql,
            (item_id, project_id, request.type, request.content, title),
        )
        check_db_result(result, "add project item")
        audit_log("CREATE", "project_item", item_id)

        return item_id

    async def get_items(self, project_id: str) -> list[dict[str, Any]]:
        """List all items for a project."""
        sql = """
        SELECT id, projectId, type, content, title, createdAt as created_at
        FROM dbo.ProjectItems
        WHERE projectId = ?
        ORDER BY createdAt DESC
        """
        try:
            items_data = await execute_query(self.settings, sql, (project_id,))
            return [
                {
                    "id": str(item.get("id")),
                    "project_id": str(item.get("projectId")),
                    "type": item.get("type"),
                    "content": item.get("content", ""),
                    "title": item.get("title"),
                    "created_at": item.get("created_at"),
                }
                for item in items_data
            ]
        except Exception as e:
            logger.error("Error fetching project items: %s", e)
            return []
