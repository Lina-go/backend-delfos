"""
Agent executor for running agents in isolation.
"""

import logging
from typing import Any, TypeVar, cast

from pydantic import BaseModel, ValidationError

from src.utils.json_parser import JSONParser
from src.utils.retry import run_with_retry

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


async def run_single_agent(agent: Any, input_text: str) -> str:
    """Run a single agent in isolation using its native ``run`` method."""

    async def _execute_agent() -> str:
        response = await agent.run(input_text)

        # Logging for debugging
        logger.info("--------------------------------")
        logger.info(f"Agent Response Text: {response.text}")
        logger.info("--------------------------------")

        return str(response.text)

    result = cast(
        str,
        await run_with_retry(
            _execute_agent,
            max_retries=2,
            initial_delay=2.0,
            backoff_factor=2.0,
            retry_on_rate_limit=True,
        ),
    )
    return result


async def run_agent_with_format(
    agent: Any,
    input_text: str,
    response_format: type[T] | None = None,
) -> T | str:
    """Run an agent and parse its response into a Pydantic model.

    Validation/format errors are captured to avoid unnecessary retries.
    """

    async def _execute_agent() -> T | str:
        # 1. Execute the agent (invokes tools)
        response = await agent.run(input_text)

        # Detailed logging
        logger.info("--------------------------------")
        logger.info(f"Respuesta cruda: {response.text}")
        logger.info(f"Mensajes en historial: {len(response.messages)}")
        for message in response.messages:
            logger.info(f"Role: {message.role}, Text: {message.text}")
        logger.info("--------------------------------")

        text_result: str = str(response.text)

        if response_format and text_result:
            try:
                json_data = JSONParser.extract_json(text_result)

                if json_data:
                    return response_format(**json_data)
                else:
                    logger.warning("No valid JSON found in the response.")

            except (ValidationError, ValueError, Exception) as e:
                logger.error(
                    "Formatting/validation error (will NOT be retried): %s",
                    e,
                )
                return text_result

        return text_result

    result = cast(
        T | str,
        await run_with_retry(
            _execute_agent,
            max_retries=2,
            initial_delay=2.0,
            backoff_factor=2.0,
            retry_on_rate_limit=True,
        ),
    )
    return result
