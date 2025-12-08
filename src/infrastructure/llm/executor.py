"""
Agent executor for running agents in isolation.
"""
import logging
from typing import Any, Optional, Type, TypeVar

from pydantic import BaseModel

from src.utils.retry import run_with_retry

logger = logging.getLogger(__name__)

# Generic type for Pydantic models
T = TypeVar("T", bound=BaseModel)


async def run_single_agent(agent: Any, input_text: str) -> str:
    """
    Execute an agent using its native run() method so the agent
    manages the full tool-use loop until it returns final text.
    """

    async def _execute_agent():
        response = await agent.run(input_text)
        # Most agent clients expose response.text; fall back to str(response) otherwise
        return getattr(response, "text", str(response)) if response is not None else ""

    return await run_with_retry(
        _execute_agent,
        max_retries=2,
        initial_delay=2.0,
        backoff_factor=2.0,
        retry_on_rate_limit=True,
    )


async def run_agent_with_format(
    agent: Any,
    input_text: str,
    response_format: Optional[Type[T]] = None,
) -> T | str:
    """
    Execute agent via native run() and parse JSON into the desired Pydantic model.
    Returns text if parsing fails or no format is requested.
    """

    async def _execute_agent():
        response = await agent.run(input_text)
        # Prefer .text; fall back to .value; finally to repr/str
        text_result = ""
        if response is not None:
            # Try common response fields in order
            for attr in ["text", "value", "output", "output_text", "content"]:
                text_result = getattr(response, attr, "") or ""
                if text_result:
                    break
            if not text_result:
                # last resort: repr/str to help debugging
                text_result = repr(response)

        if response_format and text_result:
            from src.utils.json_parser import JSONParser

            json_data = JSONParser.extract_json(text_result)
            if json_data:
                try:
                    return response_format(**json_data)
                except Exception as e:
                    logger.warning(f"Failed to parse response as {response_format.__name__}: {e}")
                    return text_result
            else:
                logger.warning("No JSON found in response")

        return text_result

    return await run_with_retry(
        _execute_agent,
        max_retries=2,
        initial_delay=2.0,
        backoff_factor=2.0,
        retry_on_rate_limit=True,
    )