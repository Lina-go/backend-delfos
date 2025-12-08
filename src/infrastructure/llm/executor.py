"""
Agent executor for running agents in isolation.
"""
import logging
from typing import Any, TypeVar, Optional, Type
from pydantic import BaseModel

# Importamos la utilidad de reintento
from src.utils.retry import run_with_retry
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)

# Definimos el tipo genérico para modelos Pydantic
T = TypeVar("T", bound=BaseModel)

async def run_single_agent(agent: Any, input_text: str) -> str:
    """
    Ejecuta un agente en aislamiento usando su método nativo run().
    Esto maneja automáticamente el ciclo de herramientas y se detiene al finalizar.
    """
    async def _execute_agent():
        response = await agent.run(input_text)
        print("--------------------------------")
        print(response.text)
        print(len(response.messages))
        print("MENSAJES:")
        for message in response.messages:
            print(f"Role: {message.role}, Text: {message.text}")
        print("--------------------------------")
        return response.text
    
    # Ejecutamos con lógica de reintento para rate limits
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
        print("--------------------------------")
        print(response.text)
        print(len(response.messages))
        print("MENSAJES:")
        for message in response.messages:
            print(f"Role: {message.role}, Text: {message.text}")
        print("--------------------------------")
        text_result = response.text 

        if response_format and text_result:

            
            json_data = JSONParser.extract_json(text_result)
            
            if json_data:
                try:
                    return response_format(**json_data)
                except Exception as e:
                    logger.warning(f"Error al parsear respuesta como {response_format.__name__}: {e}")
                    # Si falla el parseo, devolvemos el texto para no romper el flujo
                    return text_result
            else:
                logger.warning("No se encontró JSON válido en la respuesta")
        
        return text_result
    
    return await run_with_retry(
        _execute_agent,
        max_retries=2,
        initial_delay=2.0,
        backoff_factor=2.0,
        retry_on_rate_limit=True,
    )