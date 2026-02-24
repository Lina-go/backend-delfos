"""API sub-routers assembled into a single api_router."""

from fastapi import APIRouter

from src.api.routers.advisor import router as advisor_router
from src.api.routers.cache import router as cache_router
from src.api.routers.chat import router as chat_router
from src.api.routers.chat_v2 import router as chat_v2_router
from src.api.routers.graphs import router as graphs_router
from src.api.routers.informes import router as informes_router
from src.api.routers.projects import router as projects_router

api_router = APIRouter()

api_router.include_router(chat_router, tags=["chat"])
api_router.include_router(chat_v2_router, prefix="/v2", tags=["chat_v2"])
api_router.include_router(graphs_router, prefix="/graphs", tags=["graphs"])
api_router.include_router(projects_router, prefix="/projects", tags=["projects"])
api_router.include_router(informes_router, prefix="/informes", tags=["informes"])
api_router.include_router(cache_router, prefix="/cache", tags=["cache"])
api_router.include_router(advisor_router, prefix="/advisor", tags=["advisor"])
