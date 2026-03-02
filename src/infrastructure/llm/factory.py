"""Agent factory for Azure AI and Anthropic backends."""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from agent_framework.anthropic import AnthropicClient
from agent_framework_azure_ai import AzureAIAgentClient
from anthropic import AsyncAnthropicFoundry
from azure.core.credentials_async import AsyncTokenCredential
from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential

from src.config.settings import Settings

logger = logging.getLogger(__name__)

_shared_credential: AsyncTokenCredential | None = None
_shared_anthropic_client: Any = None


def get_shared_credential(settings: Settings | None = None) -> AsyncTokenCredential:
    """Return the shared async credential singleton, creating it on first call."""
    global _shared_credential
    if _shared_credential is not None:
        if settings and settings.use_service_principal and isinstance(_shared_credential, DefaultAzureCredential):
            logger.warning(
                "get_shared_credential() called with use_service_principal=True, "
                "but a DefaultAzureCredential was already created. "
                "The existing credential will be reused."
            )
        return _shared_credential

    if settings and settings.use_service_principal:
        logger.info("LLM credential: Service Principal (ClientSecretCredential)")
        _shared_credential = ClientSecretCredential(
            tenant_id=settings.azure_tenant_id,
            client_id=settings.azure_client_id,
            client_secret=settings.azure_client_secret,
        )
    else:
        logger.info("LLM credential: DefaultAzureCredential (Managed Identity)")
        _shared_credential = DefaultAzureCredential(
            exclude_visual_studio_code_credential=True,
            exclude_powershell_credential=True,
            exclude_developer_cli_credential=True,
        )
    return _shared_credential


async def close_shared_credential() -> None:
    """Close and discard the shared credential singleton."""
    global _shared_credential
    if _shared_credential is not None:
        await _shared_credential.close()
        _shared_credential = None


async def close_shared_anthropic_client() -> None:
    """Close and discard the shared Anthropic HTTP client."""
    global _shared_anthropic_client  # noqa: PLW0603
    if _shared_anthropic_client is not None:
        await _shared_anthropic_client.close()
        _shared_anthropic_client = None


def is_anthropic_model(model: str) -> bool:
    """Check if model is Anthropic (Claude)."""
    return "claude" in model.lower()


@asynccontextmanager
async def azure_agent_client(
    settings: Settings,
    model: str,
    credential: AsyncTokenCredential,
    max_iterations: int = 5,
) -> Any:
    """Yield a configured Azure AI Foundry agent client."""
    async with AzureAIAgentClient(
        project_endpoint=settings.azure_ai_project_endpoint,
        model_deployment_name=model,
        async_credential=credential,
    ) as client:
        if client.function_invocation_configuration is not None:
            client.function_invocation_configuration.max_iterations = max_iterations
            client.function_invocation_configuration.include_detailed_errors = True
        yield client


def warmup_anthropic_client(settings: Settings) -> None:
    """Pre-initialize the shared Anthropic HTTP client. Called once at startup."""
    _get_shared_anthropic_http_client(settings)


def _get_shared_anthropic_http_client(settings: Settings) -> Any:
    """Return a singleton Anthropic HTTP client (direct or Foundry).

    Sharing the underlying HTTP client across AnthropicClient instances
    reuses TCP/TLS connections, avoiding cold-start handshake on each request.
    """
    global _shared_anthropic_client  # noqa: PLW0603
    if _shared_anthropic_client is not None:
        return _shared_anthropic_client

    if settings.use_anthropic_api_for_claude:
        if not settings.anthropic_api_key:
            raise ValueError("use_anthropic_api_for_claude is True but ANTHROPIC_API_KEY is not set")
        from anthropic import AsyncAnthropic
        _shared_anthropic_client = AsyncAnthropic(api_key=settings.anthropic_api_key)
        logger.info("Shared Anthropic HTTP client created (direct API)")
    else:
        foundry_api_key = os.getenv("ANTHROPIC_FOUNDRY_API_KEY") or settings.anthropic_foundry_api_key
        foundry_resource = os.getenv("ANTHROPIC_FOUNDRY_RESOURCE") or settings.anthropic_foundry_resource
        if not foundry_api_key:
            raise ValueError("ANTHROPIC_FOUNDRY_API_KEY is required.")
        _shared_anthropic_client = AsyncAnthropicFoundry(
            api_key=foundry_api_key, resource=foundry_resource,
        )
        logger.info("Shared Anthropic HTTP client created (Foundry)")

    return _shared_anthropic_client


def _build_anthropic_client(settings: Settings, model: str) -> AnthropicClient:
    """Build an AnthropicClient reusing the shared HTTP connection pool."""
    shared = _get_shared_anthropic_http_client(settings)
    return AnthropicClient(anthropic_client=shared, model_id=model)


def create_claude_agent(
    settings: Settings,
    name: str,
    instructions: str,
    tools: Any | None = None,
    model: str | None = None,
    max_tokens: int = 8192,
    response_format: Any | None = None,
) -> Any:
    """Create a Claude agent via direct API or Foundry."""
    final_model = model or settings.sql_agent_model
    logger.debug("Creating Claude agent '%s' with model: %s", name, final_model)

    client = _build_anthropic_client(settings, final_model)
    agent_kwargs: dict[str, Any] = {
        "name": name,
        "instructions": instructions,
        "tools": tools,
        "max_tokens": max_tokens,
        "seed": 42,
        "temperature": 0.0,
    }
    if response_format:
        agent_kwargs["response_format"] = response_format

    return client.create_agent(**agent_kwargs)


# Backward-compatible aliases
create_anthropic_agent = create_claude_agent
create_anthropic_foundry_agent = create_claude_agent
