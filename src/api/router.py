"""Backward-compatible shim.

The monolithic router has been split into sub-routers under
``src.api.routers`` and service classes under ``src.services``.
This module re-exports the assembled ``api_router`` as ``router``
so that existing imports (e.g. ``from src.api.router import router``)
continue to work without modification.
"""

from src.api.routers import api_router as router

__all__ = ["router"]
