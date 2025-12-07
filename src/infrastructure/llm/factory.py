"""Agent factory helpers."""

from typing import Any
from contextlib import asynccontextmanager

from azure.identity.aio import DefaultAzureCredential
from agent_framework_azure_ai import AzureAIAgentClient
from agent_framework.anthropic import AnthropicClient

from src.config.settings import Settings


def is_anthropic_model(model: str) -> bool:
    """Check if model is Anthropic (Claude)."""
    return "claude" in model.lower()


@asynccontextmanager
async def azure_agent_client(
    settings: Settings,
    model: str,
    credential: DefaultAzureCredential,
):
    """
    Azure AI agent client as context manager.
    
    Usage:
        async with azure_agent_client(settings, model, credential) as client:
            agent = client.create_agent(name="X", instructions="...")
            response = await run_single_agent(agent, input)
    """
    async with AzureAIAgentClient(
        project_endpoint=settings.azure_ai_project_endpoint,
        model_deployment_name=model,
        async_credential=credential,
    ) as client:
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
        agent = create_anthropic_agent(settings, "SQLAgent", prompt, mcp)
        response = await run_single_agent(agent, input)
    """
    client = AnthropicClient(
        model_id=model or settings.sql_agent_model,
        api_key=settings.anthropic_api_key,
    )
    return client.create_agent(
        name=name,
        instructions=instructions,
        tools=tools,
        max_tokens=max_tokens,
        response_format=response_format,
    )