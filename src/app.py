"""FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.router import router
from src.config.settings import get_settings
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    logger.info("Starting Delfos NL2SQL Pipeline")
    yield
    logger.info("Shutting down Delfos NL2SQL Pipeline")
    try:
        ConnectionPool.close_pool()
        logger.info("Database connection pool closed")
    except Exception as e:
        logger.error(f"Error closing connection pool: {e}", exc_info=True)
    try:
        await close_shared_credential()
        logger.info("Shared credential closed")
    except Exception as e:
        logger.error(f"Error closing shared credential: {e}", exc_info=True)


app = FastAPI(
    title="Delfos NL2SQL Pipeline",
    description="Natural Language to SQL pipeline with multi-step orchestration",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")