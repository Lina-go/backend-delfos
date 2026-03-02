"""Background keep-alive for Azure Fabric connection pools.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.infrastructure.database.connection import ConnectionPool
    from src.infrastructure.database.tools import DelfosTools

logger = logging.getLogger(__name__)


class PoolKeepAlive:
    """Daemon thread that periodically pings all idle pool connections.

    Parameters
    ----------
    main_pools:
        ``ConnectionPool`` instances (WH and DB pools from ``connection.py``).
    agent_tools:
        The ``DelfosTools`` singleton (or ``None``).  Its internal pools
        are pinged separately.
    interval_seconds:
        Ping sweep interval.  Default 210 s (3.5 min) stays below Azure's
        ~4 min idle-TCP timeout.
    """

    def __init__(
        self,
        main_pools: list[ConnectionPool],
        agent_tools: DelfosTools | None = None,
        interval_seconds: int = 210,
    ) -> None:
        self._main_pools = main_pools
        self._agent_tools = agent_tools
        self._interval = interval_seconds
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="db-keepalive", daemon=True,
        )

    def start(self) -> None:
        logger.info("DB keep-alive starting (interval=%ds)", self._interval)
        self._thread.start()

    def stop(self) -> None:
        logger.info("DB keep-alive stopping")
        self._stop_event.set()
        self._thread.join(timeout=self._interval + 5)

    def _run(self) -> None:
        while not self._stop_event.wait(timeout=self._interval):
            self._sweep()

    def _sweep(self) -> None:
        total_pinged = 0
        total_replaced = 0

        for pool in self._main_pools:
            pinged, replaced = pool.ping_idle_connections()
            total_pinged += pinged
            total_replaced += replaced

        if self._agent_tools is not None:
            pinged, replaced = self._agent_tools.ping_idle_connections()
            total_pinged += pinged
            total_replaced += replaced

        logger.info(
            "DB keep-alive sweep: %d pinged, %d replaced", total_pinged, total_replaced,
        )
