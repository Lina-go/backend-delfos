"""Shared LLM helper for handler modules.

Lazy imports are used throughout to avoid circular dependencies between
the orchestrator and infrastructure layers.
"""

from typing import Any

from src.config.settings import Settings


def _lazy_imports() -> tuple:
    """Return (azure_agent_client, get_shared_credential, run_single_agent, run_agent_with_format)."""
    from src.infrastructure.llm.executor import run_agent_with_format, run_single_agent
    from src.infrastructure.llm.factory import azure_agent_client, get_shared_credential

    return azure_agent_client, get_shared_credential, run_single_agent, run_agent_with_format


def _build_agent_kwargs(
    name: str,
    instructions: str,
    max_tokens: int,
    temperature: float,
    tools: list | None,
    response_format: type | None = None,
) -> dict[str, Any]:
    """Build the keyword arguments dict for ``client.create_agent``."""
    kwargs: dict[str, Any] = {
        "name": name,
        "instructions": instructions,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if tools is not None:
        kwargs["tools"] = tools
    if response_format is not None:
        kwargs["response_format"] = response_format
    return kwargs


async def run_handler_agent(
    settings: Settings,
    name: str,
    instructions: str,
    message: str,
    *,
    model: str | None = None,
    tools: list | None = None,
    max_iterations: int = 2,
    max_tokens: int = 1024,
    temperature: float = 0.7,
) -> str:
    """Run an Azure agent and return its plain-text response."""
    azure_agent_client, get_shared_credential, run_single_agent, _ = _lazy_imports()

    credential = get_shared_credential(settings)
    agent_model = model or settings.triage_agent_model
    async with azure_agent_client(
        settings, agent_model, credential, max_iterations=max_iterations
    ) as client:
        agent = client.create_agent(
            **_build_agent_kwargs(name, instructions, max_tokens, temperature, tools)
        )
        return await run_single_agent(agent, message)


async def run_formatted_handler_agent(
    settings: Settings,
    name: str,
    instructions: str,
    message: str,
    *,
    response_format: type,
    model: str | None = None,
    tools: list | None = None,
    max_iterations: int = 2,
    max_tokens: int = 1024,
    temperature: float = 0.7,
) -> Any:
    """Run an Azure agent and return its response parsed into *response_format*."""
    azure_agent_client, get_shared_credential, _, run_agent_with_format = _lazy_imports()

    credential = get_shared_credential(settings)
    agent_model = model or settings.triage_agent_model
    async with azure_agent_client(
        settings, agent_model, credential, max_iterations=max_iterations
    ) as client:
        agent = client.create_agent(
            **_build_agent_kwargs(name, instructions, max_tokens, temperature, tools, response_format)
        )
        return await run_agent_with_format(agent, message, response_format=response_format)
