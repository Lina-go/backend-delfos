"""
Agent executor for running agents in isolation.
"""
import logging
from typing import Any, TypeVar, Optional, Type
from pydantic import BaseModel, ValidationError

from src.utils.retry import run_with_retry
from src.utils.json_parser import JSONParser

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

async def run_single_agent(agent: Any, input_text: str) -> str:
    """
    Ejecuta un agente en aislamiento usando su método nativo run().
    """
    async def _execute_agent():
        response = await agent.run(input_text)
        
        # Logging para depuración
        logger.info("--------------------------------")
        logger.info(f"Agent Response Text: {response.text}")
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
    Captura errores de validación para evitar reintentos innecesarios.
    """
    async def _execute_agent():
        # 1. Ejecutar el agente (invoca herramientas)
        response = await agent.run(input_text)
        
        # Logging detallado
        logger.info("--------------------------------")
        logger.info(f"Respuesta cruda: {response.text}")
        logger.info(f"Mensajes en historial: {len(response.messages)}")
        for message in response.messages:
            logger.info(f"Role: {message.role}, Text: {message.text}")
        logger.info("--------------------------------")
        
        text_result = response.text 

        # 2. Intentar parsear de forma segura
        # El try/except aquí es CLAVE para evitar que run_with_retry ejecute todo de nuevo
        if response_format and text_result:
            try:
                json_data = JSONParser.extract_json(text_result)
                
                if json_data:
                    # Validar con Pydantic
                    return response_format(**json_data)
                else:
                    logger.warning("No se encontró JSON válido en la respuesta.")
            
            except (ValidationError, ValueError, Exception) as e:
                # Si falla el parseo, LOGUEAMOS pero NO lanzamos el error.
                # Devolvemos el texto crudo para que el pipeline continúe o falle suavemente.
                logger.error(f"Error de formato/validación (NO se reintentará): {e}")
                return text_result

        return text_result

    # run_with_retry solo reintentará errores de red o del modelo,
    # no errores de formato de salida.
    return await run_with_retry(
        _execute_agent,
        max_retries=2,
        initial_delay=2.0,
        backoff_factor=2.0,
        retry_on_rate_limit=True,
    )