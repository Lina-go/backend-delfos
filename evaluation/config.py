"""Evaluation configuration."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class EvalConfig:
    """Configuration for evaluation runs."""

    # Paths
    data_path: Path = Path(__file__).parent / "data" / "queries.csv"
    results_dir: Path = Path(__file__).parent / "results"

    # Execution settings
    delay_between_queries: float = 2.0
    timeout_per_query: float = 120.0

    # Run identification
    run_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))

    def __post_init__(self) -> None:
        """Ensure directories exist."""
        self.results_dir.mkdir(parents=True, exist_ok=True)

    @property
    def output_path(self) -> Path:
        """Path for results JSON."""
        return self.results_dir / f"{self.run_id}_results.json"