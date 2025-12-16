"""Main evaluation script - generates JSON results."""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from evaluation.config import EvalConfig
from evaluation.executor import Executor
from evaluation.loader import load_queries_with_sql, sample_queries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def evaluate_query(executor: Executor, query: Any) -> dict[str, Any]:
    """Evaluate a single query."""
    logger.info(f"[{query.id}] {query.question[:60]}...")

    # Run pipeline
    pipeline = await executor.run_pipeline(query.question)

    # Run gold SQL
    gold = await executor.run_gold_sql(query.sql)

    # Combine errors
    error = pipeline.get("error") or gold.get("error")

    return {
        "id": query.id,
        "question": query.question,
        "gold_archetype": query.archetype,
        "gold_sql": query.sql,
        "gold_difficulty": query.difficulty,
        "gold_tables": query.gold_tables,
        "gold_results": gold.get("gold_results"),
        "predicted_archetype": pipeline.get("predicted_archetype"),
        "predicted_sql": pipeline.get("predicted_sql"),
        "predicted_results": pipeline.get("predicted_results"),
        "error": error,
    }


async def run_evaluation(config: EvalConfig, sample_size: int | None = None) -> dict[str, Any]:
    """Run evaluation and return results."""
    queries = load_queries_with_sql(config.data_path)
    logger.info(f"Loaded {len(queries)} queries with SQL")

    if sample_size:
        queries = sample_queries(queries, n=sample_size)
        logger.info(f"Sampled {len(queries)} queries")

    results: list[dict[str, Any]] = []
    async with Executor() as executor:
        for i, query in enumerate(queries):
            try:
                result = await evaluate_query(executor, query)
                results.append(result)
                logger.info(f"[{i + 1}/{len(queries)}] Done")

                if i < len(queries) - 1:
                    await asyncio.sleep(config.delay_between_queries)

            except Exception as e:
                logger.error(f"Error on query {query.id}: {e}")
                results.append(
                    {
                        "id": query.id,
                        "question": query.question,
                        "gold_archetype": query.archetype,
                        "gold_sql": query.sql,
                        "gold_difficulty": query.difficulty,
                        "gold_tables": query.gold_tables,
                        "gold_results": None,
                        "predicted_archetype": None,
                        "predicted_sql": None,
                        "predicted_results": None,
                        "error": str(e),
                    }
                )

    return {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "total_queries": len(results),
            "dataset": config.data_path.name,
        },
        "results": results,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run NL2SQL evaluation")
    parser.add_argument("--sample", type=int, help="Number of queries to sample")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between queries")
    parser.add_argument("--output", type=str, help="Output JSON path")
    args = parser.parse_args()

    config = EvalConfig(delay_between_queries=args.delay)
    output = await run_evaluation(config, sample_size=args.sample)

    output_path = Path(args.output) if args.output else config.output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=str)

    logger.info(f"Results saved to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
