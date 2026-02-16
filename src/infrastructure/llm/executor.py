"""Agent executor for running agents in isolation."""

import asyncio
import logging
from typing import Any, TypeVar

from agent_framework import ChatMessage
from pydantic import BaseModel

from src.config.settings import get_settings
from src.utils.json_parser import JSONParser
from src.utils.retry import run_with_retry

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

_MAX_CONCURRENT = max(1, get_settings().llm_max_concurrent_requests)
_LLM_SEMAPHORE = asyncio.Semaphore(_MAX_CONCURRENT)


async def _execute_with_retry(agent: Any, input_text: str) -> Any:
    """Run an agent under semaphore with retry logic."""

    async def _run() -> Any:
        async with _LLM_SEMAPHORE:
            return await agent.run(input_text)

    return await run_with_retry(
        _run,
        max_retries=2,
        initial_delay=2.0,
        backoff_factor=2.0,
        retry_on_rate_limit=True,
    )


def _try_parse_response(text: str, response_format: type[T]) -> T | None:
    """Try to parse text into a Pydantic model. Returns None on failure."""
    try:
        json_data = JSONParser.extract_json(text)
        if json_data and any(v is not None for v in json_data.values()):
            return response_format(**json_data)
    except Exception as e:
        logger.warning("Parse attempt failed: %s", e)
    return None


async def _prefill_json_retry(
    agent: Any, narrative: str, response_format: type[T]
) -> T | None:
    """Use Anthropic prefill technique to force JSON output.

    Passes the agent's narrative response as context, then prefills
    with '{' to force Claude to output JSON immediately.
    See: how_to_enable_json_mode.ipynb
    """
    messages = [
        ChatMessage(
            role="user",
            text=(
                "Your previous response was analysis text instead of the required JSON. "
                "Based on your analysis below, output ONLY the JSON:\n\n"
                f"{narrative[:3000]}"
            ),
        ),
        ChatMessage(role="assistant", text="{"),
    ]
    try:
        async with _LLM_SEMAPHORE:
            retry_response = await agent.run(messages=messages, tool_choice="none")
        retry_text = "{" + str(retry_response.text)
        return _try_parse_response(retry_text, response_format)
    except Exception as e:
        logger.error("Prefill retry error: %s", e)
        return None


async def run_single_agent(agent: Any, input_text: str) -> str:
    """Run a single agent in isolation using its native ``run`` method."""
    response = await _execute_with_retry(agent, input_text)
    logger.info("Agent Response Text: %s", response.text)
    return str(response.text)


async def run_agent_with_format(
    agent: Any,
    input_text: str,
    response_format: type[T] | None = None,
) -> T | str:
    """Run an agent and parse its response into a Pydantic model."""
    response = await _execute_with_retry(agent, input_text)

    logger.info("Raw agent response: %s", response.text)
    text_result: str = str(response.text)

    if not response_format or not text_result:
        return text_result

    parsed = _try_parse_response(text_result, response_format)
    if parsed is not None:
        return parsed

    logger.warning("No valid JSON in agent response. Using prefill retry.")
    retry_result = await _prefill_json_retry(agent, text_result, response_format)
    if retry_result is not None:
        return retry_result

    logger.error("Prefill retry also failed. Returning raw text.")
    return text_result
