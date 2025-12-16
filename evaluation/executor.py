"""Pipeline executor for evaluation."""

import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import get_settings
from src.infrastructure.mcp.client import MCPClient
from src.orchestrator.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)


class Executor:
    """Executes pipeline and gold SQL queries."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._orchestrator: PipelineOrchestrator | None = None

    async def __aenter__(self) -> "Executor":
        self._orchestrator = PipelineOrchestrator(self.settings)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._orchestrator:
            await self._orchestrator.close()

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

        try:
            async with MCPClient(self.settings) as mcp:
                result = await mcp.execute_sql(sql)

                if result.get("error"):
                    return {"error": result["error"]}

                raw = result.get("data", [])
                return {"gold_results": self._parse_results(sql, raw)}

        except Exception as e:
            logger.error(f"Gold SQL error: {e}")
            return {"error": str(e)}

    def _parse_results(self, sql: str, raw: list[str]) -> list[dict]:
        """Parse raw MCP results to list of dicts."""
        import ast
        import re

        if not raw:
            return []

        # Extract columns from SELECT
        match = re.search(r"SELECT\s+(.+?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
        columns = []
        if match:
            for part in match.group(1).split(","):
                part = part.strip()
                as_match = re.search(r"\bAS\s+(\w+)\s*$", part, re.IGNORECASE)
                if as_match:
                    columns.append(as_match.group(1))
                else:
                    col = part.split()[-1].split(".")[-1].strip("[]")
                    columns.append(col)

        results = []
        for row_str in raw:
            try:
                values = list(ast.literal_eval(row_str.strip()))
            except (ValueError, SyntaxError):
                continue

            if columns:
                row = {columns[i]: values[i] for i in range(min(len(columns), len(values)))}
            else:
                row = {f"col_{i}": v for i, v in enumerate(values)}
            results.append(row)

        return results