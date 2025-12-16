"""Dataset loader for evaluation queries."""

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Query:
    """A single evaluation query."""

    id: int
    question: str
    sql: str
    archetype: str
    difficulty: str
    tables_used: str

    @property
    def has_sql(self) -> bool:
        """Check if query has gold SQL."""
        return bool(self.sql and self.sql.strip())

    @property
    def gold_tables(self) -> list[str]:
        """Parse tables_used into list."""
        if not self.tables_used:
            return []
        return [f"dbo.{t.strip()}" for t in self.tables_used.split(",")]


def load_queries(path: Path) -> list[Query]:
    """Load queries from CSV file."""
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    queries = []

    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for idx, row in enumerate(reader):
            query = Query(
                id=idx,
                question=row.get("question_es", "").strip(),
                sql=row.get("sql", "").strip(),
                archetype=row.get("archetype", "").strip().upper(),
                difficulty=row.get("difficulty", "").strip().lower(),
                tables_used=row.get("tables_used", "").strip(),
            )

            if query.question:
                queries.append(query)

    logger.info(f"Loaded {len(queries)} queries from {path}")
    return queries


def load_queries_with_sql(path: Path) -> list[Query]:
    """Load only queries that have gold SQL."""
    return [q for q in load_queries(path) if q.has_sql]


def sample_queries(queries: list[Query], n: int = 10) -> list[Query]:
    """Sample n queries stratified by archetype."""
    if n >= len(queries):
        return queries

    groups: dict[str, list[Query]] = {}
    for q in queries:
        groups.setdefault(q.archetype, []).append(q)

    samples_per_group = max(1, n // len(groups))
    sampled: list[Query] = []

    for group_queries in groups.values():
        sampled.extend(group_queries[:samples_per_group])

    return sampled[:n]
