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
    Accumulates all text chunks from streaming response to ensure complete output.
    """
    async def _execute_agent():
        workflow = SequentialBuilder().participants([agent]).build()
        full_text = ""
        async for event in workflow.run_stream(input_text):
            if isinstance(event, WorkflowOutputEvent):
                for msg in event.data:
                    if hasattr(msg, "text") and msg.text:
                        full_text += msg.text
        return full_text if full_text else ""
    
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
        full_text = ""  # Accumulate all text chunks
        async for event in workflow.run_stream(input_text):
            if isinstance(event, WorkflowOutputEvent):
                for msg in event.data:
                    if hasattr(msg, "text") and msg.text:
                        # Accumulate text chunks instead of overwriting
                        full_text += msg.text
        
        logger.debug(f"run_agent_with_format: full_text length={len(full_text)}, response_format={response_format}")
        if not full_text:
            logger.warning(f"run_agent_with_format: No text received from agent for {response_format.__name__ if response_format else 'no format'}")
        
        # If response_format provided, parse and validate
        if response_format and full_text:
            from src.utils.json_parser import JSONParser
            json_data = JSONParser.extract_json(full_text)
            if json_data:
                try:
                    parsed_model = response_format(**json_data)
                    logger.debug(f"Successfully parsed {response_format.__name__} from response")
                    return parsed_model
                except Exception as e:
                    logger.warning(
                        f"Failed to parse response as {response_format.__name__}: {e}. "
                        f"JSON data: {json_data}. Full text (first 500 chars): {full_text[:500]}"
                    )
                    return full_text
            else:
                logger.warning(
                    f"Could not extract JSON from response for {response_format.__name__}. "
                    f"Full text (first 500 chars): {full_text[:500]}"
                )
                return full_text
        
        result = full_text if full_text else ""
        logger.debug(f"run_agent_with_format: returning {type(result).__name__}, length={len(result) if isinstance(result, str) else 'N/A'}")
        return result
    
    # Execute with retry logic for rate limits
    return await run_with_retry(
        _execute_agent,
        max_retries=2,
        initial_delay=2.0,
        backoff_factor=2.0,
        retry_on_rate_limit=True,
    )