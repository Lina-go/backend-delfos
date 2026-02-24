"""Pattern hooks -- extensible per-sub_type customizations."""

from src.patterns.registry import PatternHooks, get_hooks, register

# Register all pattern hooks on import.
import src.patterns.relacion  # noqa: F401
import src.patterns.comparacion  # noqa: F401

__all__ = ["PatternHooks", "get_hooks", "register"]
