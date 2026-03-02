"""Delfos backend"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from src.api.routers import api_router
from src.config.settings import Settings, get_settings
from src.infrastructure.database.connection import ConnectionPool, close_shared_sync_credential
from src.infrastructure.database.keepalive import PoolKeepAlive
from src.infrastructure.llm.factory import (
    close_shared_anthropic_client,
    close_shared_credential,
    warmup_anthropic_client,
)
from src.infrastructure.logging.logger import setup_logging
from src.services.advisor.agent import warmup_credential as warmup_advisor
from src.services.chat_v2.agent import warmup_cache, warmup_tools

settings = get_settings()

setup_logging(
    level=settings.log_level,
    json_output=not settings.debug,
    silence_noisy_loggers=True,
)

logger = logging.getLogger(__name__)


##############################################################################
# Startup helpers
##############################################################################


def _validate_startup_config(settings: Settings) -> None:
    """Warn about missing API keys or DB server config."""
    has_api_key = any((
        settings.anthropic_api_key,
        settings.anthropic_foundry_api_key,
        settings.azure_ai_project_endpoint,
    ))
    if not has_api_key:
        logger.warning(
            "No AI API key configured "
            "(anthropic_api_key, anthropic_foundry_api_key, or azure_ai_project_endpoint)"
        )

    if settings.use_direct_db and (not settings.wh_server or not settings.db_server):
        logger.warning("wh_server or db_server is empty")


def _warmup_db_and_keepalive(settings: Settings) -> PoolKeepAlive | None:
    """Init DB/WH pools and DelfosTools"""
    db_pool = wh_pool = None
    try:
        db_pool = ConnectionPool.get_db_pool(settings)
        wh_pool = ConnectionPool.get_wh_pool(settings)
        logger.info("Connection pools initialised (DB + WH)")
    except Exception as e:
        logger.warning("Failed to pre-initialise connection pools: %s", e)

    delfos_tools = None
    try:
        delfos_tools = warmup_tools(settings)
        logger.info("DelfosTools singleton pre-initialized")
    except Exception as e:
        logger.warning("DelfosTools pre-init failed (non-fatal): %s", e)

    main_pools = [pool for pool in (db_pool, wh_pool) if pool is not None]
    if not main_pools and delfos_tools is None:
        return None

    keep_alive = PoolKeepAlive(
        main_pools=main_pools,
        agent_tools=delfos_tools,
        interval_seconds=210,
    )
    keep_alive.start()
    return keep_alive


def _warmup_singletons(settings: Settings) -> None:
    """Pre-init credentials, semantic cache, and Anthropic HTTP client."""
    try:
        warmup_advisor(settings)
        logger.info("Advisor credential pre-initialized")
    except Exception as e:
        logger.warning("Advisor credential pre-init failed (non-fatal): %s", e)

    try:
        warmup_cache(settings)
        logger.info("Semantic cache pre-initialized and warmed up")
    except Exception as e:
        logger.warning("Semantic cache pre-init failed (non-fatal): %s", e)

    try:
        warmup_anthropic_client(settings)
        logger.info("Shared Anthropic HTTP client pre-initialized")
    except Exception as e:
        logger.warning("Anthropic HTTP client pre-init failed (non-fatal): %s", e)


##############################################################################
# Shutdown
##############################################################################


async def _shutdown(keep_alive: PoolKeepAlive | None) -> None:
    """Release all shared resources in order."""
    if keep_alive is not None:
        keep_alive.stop()

    try:
        ConnectionPool.close_all_pools()
        logger.info("Connection pools closed")
    except Exception as e:
        logger.error("Error closing connection pools: %s", e, exc_info=True)

    try:
        close_shared_sync_credential()
        logger.info("Shared sync credential closed")
    except Exception as e:
        logger.error("Error closing shared sync credential: %s", e, exc_info=True)

    try:
        await close_shared_credential()
        logger.info("Shared async credential closed")
    except Exception as e:
        logger.error("Error closing shared async credential: %s", e, exc_info=True)

    try:
        await close_shared_anthropic_client()
        logger.info("Shared Anthropic HTTP client closed")
    except Exception as e:
        logger.error("Error closing shared Anthropic HTTP client: %s", e, exc_info=True)


##############################################################################
# Lifespan
##############################################################################


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Application startup and shutdown lifecycle."""
    logger.info("Starting Delfos backend v%s", settings.app_version)
    _validate_startup_config(settings)

    keep_alive: PoolKeepAlive | None = None
    if settings.use_direct_db:
        keep_alive = _warmup_db_and_keepalive(settings)

    _warmup_singletons(settings)

    yield

    logger.info("Shutting down Delfos backend")
    await _shutdown(keep_alive)


##############################################################################
# App
##############################################################################

origins_are_restricted = "*" not in settings.allowed_origins

app = FastAPI(
    title="Delfos",
    description=(
        "Delfos is a natural language to SQL pipeline that translates user "
        "queries into SQL, executes them against a database, and returns "
        "results in a user-friendly format."
    ),
    version=settings.app_version,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=origins_are_restricted,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=["*"])

app.include_router(api_router, prefix="/api")
