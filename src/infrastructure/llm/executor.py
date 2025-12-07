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
    
    Uses SequentialBuilder to ensure compatibility with all agent types (including
    those with MCP tools). Extracts structured output from the response text using
    JSON parsing and Pydantic validation.
    
    Args:
        agent: Agent instance
        input_text: Input text for the agent
        response_format: Optional Pydantic BaseModel class for structured output
        
    Returns:
        Pydantic model instance if response_format provided, otherwise str
    """
    async def _execute_agent():
        workflow = SequentialBuilder().participants([agent]).build()
        last_text = ""
        
        # Try to use agent.run() if available (for agents without tools)
        # Note: agent.run() may not exist or have different signature for all agent types
        if response_format and hasattr(agent, 'run'):
            try:
                # Check if run method accepts response_format parameter
                import inspect
                sig = inspect.signature(agent.run)
                if 'response_format' in sig.parameters:
                    result = await agent.run(input_text, response_format=response_format)
                    if hasattr(result, 'value'):
                        return result.value
                    return result
            except (AttributeError, TypeError, ValueError) as e:
                # Fall back to SequentialBuilder if agent.run() doesn't work
                logger.debug(f"agent.run() not available or incompatible: {e}, using SequentialBuilder")
                pass
        
        # Use SequentialBuilder (works with all agent types)
        async for event in workflow.run_stream(input_text):
            if isinstance(event, WorkflowOutputEvent):
                for msg in event.data:
                    if hasattr(msg, "text") and msg.text:
                        last_text = msg.text
        
        # If response_format provided, parse and validate
        if response_format and last_text:
            from src.utils.json_parser import JSONParser
            json_data = JSONParser.extract_json(last_text)
            if json_data:
                try:
                    return response_format(**json_data)
                except Exception as e:
                    logger.warning(f"Failed to parse response as {response_format.__name__}: {e}")
                    return last_text
        
        return last_text
    
    # Execute with retry logic for rate limits
    return await run_with_retry(
        _execute_agent,
        max_retries=2,
        initial_delay=2.0,
        backoff_factor=2.0,
        retry_on_rate_limit=True,
    )