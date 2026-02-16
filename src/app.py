"""FastAPI application entry point."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from src.api.routers import api_router
from src.config.settings import Settings, get_settings
from src.infrastructure.database.connection import ConnectionPool
from src.infrastructure.llm.factory import close_shared_credential
from src.infrastructure.logging.logger import setup_logging

settings = get_settings()

setup_logging(
    level=settings.log_level,
    json_output=not settings.debug,
    silence_noisy_loggers=True,
)

logger = logging.getLogger(__name__)


def _validate_startup_config(settings: Settings) -> None:
    """Validate required configuration at startup."""
    has_api_key = any(
        [
            settings.anthropic_api_key,
            settings.anthropic_foundry_api_key,
            settings.azure_ai_project_endpoint,
        ]
    )
    if not has_api_key:
        logger.warning(
            "No AI API key configured (anthropic_api_key, anthropic_foundry_api_key, or azure_ai_project_endpoint)"
        )

    if settings.use_direct_db and (not settings.wh_server or not settings.db_server):
        logger.warning("wh_server or db_server is empty — Fabric queries will fail")



@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup and shutdown lifecycle."""
    logger.info("Starting Delfos NL2SQL Pipeline")
    _validate_startup_config(settings)
    yield
    logger.info("Shutting down Delfos NL2SQL Pipeline")
    try:
        ConnectionPool.close_all_pools()
        logger.info("Database connection pools closed (WH + DB)")
    except Exception as e:
        logger.error("Error closing connection pools: %s", e, exc_info=True)
    try:
        await close_shared_credential()
        logger.info("Shared credential closed")
    except Exception as e:
        logger.error("Error closing shared credential: %s", e, exc_info=True)


app = FastAPI(
    title="Delfos NL2SQL Pipeline",
    description="Natural Language to SQL pipeline with multi-step orchestration",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials="*" not in settings.allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=["*"])

app.include_router(api_router, prefix="/api")
