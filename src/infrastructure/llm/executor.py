"""
Agent executor for running agents in isolation.
"""
import logging
from typing import Any
from pydantic import BaseModel

from agent_framework import SequentialBuilder, WorkflowOutputEvent
from src.utils.retry import run_with_retry

logger = logging.getLogger(__name__)


async def run_single_agent(agent: Any, input_text: str) -> str:
    """
    Execute an agent in isolation using SequentialBuilder.
    Includes automatic retry for rate limit errors.
    Optimized to only keep the last message instead of accumulating all.
    """
    async def _execute_agent():
        workflow = SequentialBuilder().participants([agent]).build()
        last_text = ""
        async for event in workflow.run_stream(input_text):
            if isinstance(event, WorkflowOutputEvent):
                for msg in event.data:
                    if hasattr(msg, "text") and msg.text:
                        last_text = msg.text  # Only keep the last message
        return last_text
    
    # Execute with retry logic for rate limits (optimized delays)
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
    response_format: type[BaseModel] | None = None,
) -> BaseModel | str:
    """
    Execute agent with optional structured output (response_format).
    
    Uses agent.run() directly (not SequentialBuilder) to properly support
    response_format. Returns Pydantic model from result.value when response_format
    is provided, otherwise returns string.
    
    Args:
        agent: Agent instance
        input_text: Input text for the agent
        response_format: Optional Pydantic BaseModel class for structured output
        
    Returns:
        Pydantic model instance if response_format provided, otherwise str
    """
    async def _execute_agent():
        if response_format:
            result = await agent.run(input_text, response_format=response_format)
            return result.value if hasattr(result, 'value') else result
        else:
            result = await agent.run(input_text)
            return result.value if hasattr(result, 'value') else str(result)
    
    # Execute with retry logic for rate limits
    return await run_with_retry(
        _execute_agent,
        max_retries=2,
        initial_delay=2.0,
        backoff_factor=2.0,
        retry_on_rate_limit=True,
    )