"""Shared LLM helper for handler modules.

Lazy imports are used throughout to avoid circular dependencies between
the orchestrator and infrastructure layers.

Supports both Azure AI (OpenAI models) and Anthropic (Claude models).
Model routing is automatic based on the model name.
"""

from typing import Any, NamedTuple

from src.config.settings import Settings


class _LLMDeps(NamedTuple):
    """Lazily-resolved LLM factory and executor functions."""

    azure_agent_client: Any
    create_claude_agent: Any
    get_shared_credential: Any
    is_anthropic_model: Any
    run_single_agent: Any
    run_agent_with_format: Any


def _lazy_imports() -> _LLMDeps:
    """Return LLM factory and executor functions."""
    from src.infrastructure.llm.executor import run_agent_with_format, run_single_agent
    from src.infrastructure.llm.factory import (
        azure_agent_client,
        create_claude_agent,
        get_shared_credential,
        is_anthropic_model,
    )

    return _LLMDeps(
        azure_agent_client=azure_agent_client,
        create_claude_agent=create_claude_agent,
        get_shared_credential=get_shared_credential,
        is_anthropic_model=is_anthropic_model,
        run_single_agent=run_single_agent,
        run_agent_with_format=run_agent_with_format,
    )


async def _run_with_model(
    llm: _LLMDeps,
    settings: Settings,
    name: str,
    instructions: str,
    message: str,
    executor: Any,
    model: str | None,
    tools: list | None,
    max_iterations: int,
    max_tokens: int,
    temperature: float,
    response_format: type | None = None,
) -> Any:
    """Shared implementation for both plain-text and formatted agent runs.

    *executor* is the function to call once the agent is created -- either
    ``run_single_agent`` or ``run_agent_with_format``.
    """
    agent_model = model or settings.triage_agent_model

    executor_kwargs: dict[str, Any] = {}
    if response_format is not None:
        executor_kwargs["response_format"] = response_format

    if llm.is_anthropic_model(agent_model):
        agent = llm.create_claude_agent(
            settings=settings,
            name=name,
            instructions=instructions,
            tools=tools or [],
            model=agent_model,
            max_tokens=max_tokens,
            response_format=response_format,
        )
        return await executor(agent, message, **executor_kwargs)

    credential = llm.get_shared_credential(settings)
    async with llm.azure_agent_client(
        settings, agent_model, credential, max_iterations=max_iterations
    ) as client:
        agent_kwargs: dict[str, Any] = {
            "name": name,
            "instructions": instructions,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if tools is not None:
            agent_kwargs["tools"] = tools
        if response_format is not None:
            agent_kwargs["response_format"] = response_format

        agent = client.create_agent(**agent_kwargs)
        return await executor(agent, message, **executor_kwargs)


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
    """Run an agent and return its plain-text response."""
    llm = _lazy_imports()
    return await _run_with_model(
        llm=llm,
        settings=settings,
        name=name,
        instructions=instructions,
        message=message,
        executor=llm.run_single_agent,
        model=model,
        tools=tools,
        max_iterations=max_iterations,
        max_tokens=max_tokens,
        temperature=temperature,
    )


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
    """Run an agent and return its response parsed into *response_format*."""
    llm = _lazy_imports()
    return await _run_with_model(
        llm=llm,
        settings=settings,
        name=name,
        instructions=instructions,
        message=message,
        executor=llm.run_agent_with_format,
        model=model,
        tools=tools,
        max_iterations=max_iterations,
        max_tokens=max_tokens,
        temperature=temperature,
        response_format=response_format,
    )
