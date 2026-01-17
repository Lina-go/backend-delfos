"""Agent factory helpers."""

import logging
from contextlib import asynccontextmanager
from typing import Any

from agent_framework.anthropic import AnthropicClient
from agent_framework_azure_ai import AzureAIAgentClient
from anthropic import AsyncAnthropicFoundry
from azure.identity.aio import DefaultAzureCredential

from src.config.settings import Settings

logger = logging.getLogger(__name__)

_shared_credential: DefaultAzureCredential | None = None


def get_shared_credential() -> DefaultAzureCredential:
    """
    Get or create a shared DefaultAzureCredential instance.

    This ensures all services use the same credential instance,
    avoiding duplicate credential creation and improving performance.

    Returns:
        Shared DefaultAzureCredential instance
    """
    global _shared_credential
    if _shared_credential is None:
        # In Docker, we use mounted Azure credentials from the host.
        # SharedTokenCacheCredential reads from msal_token_cache.bin
        # EnvironmentCredential reads AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
        _shared_credential = DefaultAzureCredential(
            exclude_visual_studio_code_credential=True,
            exclude_powershell_credential=True,
            exclude_developer_cli_credential=True,
        )
    return _shared_credential


async def close_shared_credential() -> None:
    """
    Close the shared credential instance.

    Should be called during application shutdown to properly clean up resources.
    """
    global _shared_credential
    if _shared_credential is not None:
        await _shared_credential.close()
        _shared_credential = None


def is_anthropic_model(model: str) -> bool:
    """Check if model is Anthropic (Claude)."""
    return "claude" in model.lower()


@asynccontextmanager
async def azure_agent_client(
    settings: Settings,
    model: str,
    credential: DefaultAzureCredential,
    max_iterations: int = 5,
) -> Any:
    """
    Azure AI (Foundry) agent client as context manager.

    The `model` argument must be the deployment name configured in your project.
    """
    async with AzureAIAgentClient(
        project_endpoint=settings.azure_ai_project_endpoint,
        model_deployment_name=model,
        async_credential=credential,
    ) as client:
        if client.function_invocation_configuration is not None:
            client.function_invocation_configuration.max_iterations = max_iterations
            client.function_invocation_configuration.include_detailed_errors = True
        yield client


def create_anthropic_agent(
    settings: Settings,
    name: str,
    instructions: str,
    tools: Any | None = None,
    model: str | None = None,
    max_tokens: int = 8192,
    response_format: Any | None = None,
) -> Any:
    """
    Create an Anthropic (Claude) agent.

    Usage:
        agent = create_anthropic_agent(settings, "SQLAgent", prompt, mcp, response_format=SQLResult)
        response = await run_single_agent(agent, input)

    Args:
        settings: Application settings
        name: Agent name
        instructions: System prompt/instructions
        tools: Optional tools (e.g., MCP connection)
        model: Optional model name (defaults to settings.sql_agent_model)
        max_tokens: Maximum tokens for response
        response_format: Optional Pydantic BaseModel for structured output
    """

    final_model = model or settings.sql_agent_model

    logger.debug(f"Creating Anthropic agent '{name}' with model: {final_model}")

    client = AnthropicClient(
        model_id=final_model,
        api_key=settings.anthropic_api_key,
    )
    agent_kwargs = {
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


def create_anthropic_foundry_agent(
    settings: Settings,
    name: str,
    instructions: str,
    tools: Any | None = None,
    model: str | None = None,
    max_tokens: int = 8192,
    response_format: Any | None = None,
) -> Any:
    """
    Create an Anthropic (Claude) agent using Anthropic on Foundry.

    Usage:
        agent = create_anthropic_foundry_agent(settings, "SQLAgent", prompt, mcp, response_format=SQLResult)
        response = await run_single_agent(agent, input)

    Args:
        settings: Application settings
        name: Agent name
        instructions: System prompt/instructions
        tools: Optional tools (e.g., MCP connection)
        model: Optional model name (defaults to settings.sql_agent_model)
        max_tokens: Maximum tokens for response
        response_format: Optional Pydantic BaseModel for structured output
    """

    final_model = model or settings.sql_agent_model

    logger.debug(f"Creating Anthropic Foundry agent '{name}' with model: {final_model}")

    # Validate that required environment variables are set
    import os
    foundry_api_key = os.getenv("ANTHROPIC_FOUNDRY_API_KEY") or settings.anthropic_foundry_api_key
    foundry_resource = os.getenv("ANTHROPIC_FOUNDRY_RESOURCE") or settings.anthropic_foundry_resource
    
    if not foundry_api_key:
        raise ValueError(
            "ANTHROPIC_FOUNDRY_API_KEY is required. Set it in environment variables or .env file."
        )
    
    # Set environment variables if they're in settings but not in environment
    if foundry_api_key and not os.getenv("ANTHROPIC_FOUNDRY_API_KEY"):
        os.environ["ANTHROPIC_FOUNDRY_API_KEY"] = foundry_api_key
    if foundry_resource and not os.getenv("ANTHROPIC_FOUNDRY_RESOURCE"):
        os.environ["ANTHROPIC_FOUNDRY_RESOURCE"] = foundry_resource

    # AsyncAnthropicFoundry reads ANTHROPIC_FOUNDRY_API_KEY and ANTHROPIC_FOUNDRY_RESOURCE
    # from environment variables automatically
    foundry_client = AsyncAnthropicFoundry()

    # Create AnthropicClient with Foundry client and model_id
    client = AnthropicClient(
        anthropic_client=foundry_client,
        model_id=final_model,
    )

    agent_kwargs = {
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
