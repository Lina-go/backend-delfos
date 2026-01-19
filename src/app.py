"""FastAPI application entry point."""

import logging
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.router import router
from src.config.settings import get_settings
from src.infrastructure.llm.factory import close_shared_credential

settings = get_settings()

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Silence verbose loggers
for logger_name in ["uvicorn", "httpx", "httpcore", "azure", "azure.core"]:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Delfos NL2SQL Pipeline",
    description="Natural Language to SQL pipeline with multi-step orchestration",
    version="0.1.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(router, prefix="/api")


@app.on_event("startup")
async def startup_event() -> None:
    """Initialize application on startup."""
    logger.info("Starting Delfos NL2SQL Pipeline")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Cleanup on shutdown."""
    logger.info("Shutting down Delfos NL2SQL Pipeline")
    # Close shared credential
    try:
        await close_shared_credential()
        logger.info("Shared credential closed")
    except Exception as e:
        logger.error(f"Error closing shared credential: {e}", exc_info=True)
