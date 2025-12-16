"""Pipeline executor for evaluation."""

import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import get_settings
from src.infrastructure.mcp.client import MCPClient
from src.orchestrator.pipeline import PipelineOrchestrator
from src.services.sql.executor import SQLExecutor

logger = logging.getLogger(__name__)


class Executor:
    """Executes pipeline and gold SQL queries."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._orchestrator: PipelineOrchestrator | None = None
        self._sql_executor: SQLExecutor | None = None

    async def __aenter__(self) -> "Executor":
        self._orchestrator = PipelineOrchestrator(self.settings)
        self._sql_executor = SQLExecutor(self.settings)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._orchestrator:
            await self._orchestrator.close()
        if self._sql_executor:
            await self._sql_executor.close()

    async def run_pipeline(self, question: str) -> dict[str, Any]:
        """Run pipeline and return predictions."""
        if not self._orchestrator:
            return {"error": "Executor not initialized"}

        try:
            predicted_archetype = None
            predicted_sql = None
            predicted_results = None

            async for event in self._orchestrator.process_stream(question, "eval_user"):
                step = event.get("step")

                if step == "intent":
                    predicted_archetype = event.get("result", {}).get("arquetipo")

                elif step == "sql_generation":
                    predicted_sql = event.get("result", {}).get("sql")

                elif step == "complete":
                    predicted_results = event.get("response", {}).get("datos")

                elif step == "error":
                    return {"error": event.get("error")}

            return {
                "predicted_archetype": predicted_archetype,
                "predicted_sql": predicted_sql,
                "predicted_results": predicted_results,
            }

        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            return {"error": str(e)}

    async def run_gold_sql(self, sql: str) -> dict[str, Any]:
        """Execute gold SQL and return results."""
        if not sql:
            return {"error": "Empty SQL"}

        if not self._sql_executor:
            return {"error": "SQLExecutor not initialized"}

        try:
            result = await self._sql_executor.execute(sql)

            # Check for errors in the result
            resumen = result.get("resumen", "")
            if resumen and ("Error" in resumen or "error" in resumen.lower()):
                return {"error": resumen}

            # Extract resultados from SQLExecutor response
            gold_results = result.get("resultados", [])
            return {"gold_results": gold_results}

        except Exception as e:
            logger.error(f"Gold SQL error: {e}")
            return {"error": str(e)}
