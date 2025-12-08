"""
Agent executor for running agents in isolation.
"""
import logging
from typing import Any, TypeVar, Optional, Type
from pydantic import BaseModel

from src.utils.retry import run_with_retry
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

async def run_single_agent(agent: Any, input_text: str) -> str:
    """
    Ejecuta un agente en aislamiento usando su método nativo run().
    Esto maneja automáticamente el ciclo de herramientas y se detiene al finalizar.
    """
    async def _execute_agent():
        response = await agent.run(input_text)
        logger.info("--------------------------------")
        logger.info(response.text)
        logger.info(len(response.messages))
        logger.info("MENSAJES:")
        for message in response.messages:
            logger.info(f"Role: {message.role}, Text: {message.text}")
        logger.info("--------------------------------")
        return response.text
    
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
    Ejecuta el agente y parsea la respuesta a un modelo Pydantic.
    """
    async def _execute_agent():
        response = await agent.run(input_text)
        logger.info("--------------------------------")
        logger.info(response.text)
        logger.info(len(response.messages))
        logger.info("MENSAJES:")
        for message in response.messages:
            logger.info(f"Role: {message.role}, Text: {message.text}")
        logger.info("--------------------------------")
        text_result = response.text 

        if response_format and text_result:
            json_data = JSONParser.extract_json(text_result)
            
            if json_data:
                return response_format(**json_data)
        return text_result

    return await run_with_retry(
        _execute_agent,
        max_retries=2,
        initial_delay=2.0,
        backoff_factor=2.0,
        retry_on_rate_limit=True,
    )