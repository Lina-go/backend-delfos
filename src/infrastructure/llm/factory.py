"""Agent factory helpers."""

from typing import Any
from contextlib import asynccontextmanager

from azure.identity.aio import DefaultAzureCredential
from agent_framework_azure_ai import AzureAIAgentClient
from agent_framework.anthropic import AnthropicClient

from src.config.settings import Settings
import logging
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
        _shared_credential = DefaultAzureCredential()
    return _shared_credential


async def close_shared_credential():
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
):
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
):
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
        "temperature": 0.0
    }
    if response_format:
        agent_kwargs["response_format"] = response_format
    
    return client.create_agent(**agent_kwargs)