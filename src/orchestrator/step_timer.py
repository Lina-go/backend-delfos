"""Async context manager for timing and logging pipeline steps."""

import json
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from src.config.constants import PipelineStep, log_pipeline_step
from src.infrastructure.logging.session_logger import SessionLogger


class StepContext:
    """Mutable context for a timed pipeline step."""

    def __init__(self) -> None:
        self.result: Any = None
        self.input_text: str | None = None
        self.system_prompt: str | None = None

    def set_result(
        self,
        result: Any,
        *,
        input_text: str | None = None,
        system_prompt: str | None = None,
    ) -> None:
        self.result = result
        if input_text is not None:
            self.input_text = input_text
        if system_prompt is not None:
            self.system_prompt = system_prompt


@asynccontextmanager
async def timed_step(
    step: PipelineStep,
    logger: SessionLogger,
    agent_name: str,
    *,
    input_text: str | None = None,
    system_prompt: str | None = None,
) -> AsyncGenerator[StepContext, None]:
    """Time a pipeline step and log its result."""
    log_pipeline_step(step)
    ctx = StepContext()
    ctx.input_text = input_text
    ctx.system_prompt = system_prompt
    start = time.time()
    yield ctx
    elapsed_ms = (time.time() - start) * 1000
    if ctx.result is not None:
        logger.log_agent_response(
            agent_name=agent_name,
            raw_response=json.dumps(ctx.result, ensure_ascii=False),
            parsed_response=ctx.result,
            input_text=ctx.input_text,
            system_prompt=ctx.system_prompt,
            execution_time_ms=elapsed_ms,
        )
