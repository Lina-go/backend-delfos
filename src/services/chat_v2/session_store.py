"""In-memory session store for Chat V2 agent threads with DB persistence."""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from agent_framework import AgentThread

from src.config.settings import Settings
from src.infrastructure.database.connection import execute_insert, execute_query

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 3600  # 1 hour


@dataclass
class ChatV2Session:
    """A single chat session tied to an AgentThread."""

    thread: AgentThread
    agent: Any = None
    context_provider: Any = None
    result_holder: dict | None = None
    created_at: float = field(default_factory=time.time)
    last_access: float = field(default_factory=time.time)
    pending_summary: str | None = None
    bg_summary_task: Any | None = None


class ChatV2SessionStore:
    """In-memory session cache backed by DB persistence in dbo.ChatV2Sessions."""

    def __init__(self) -> None:
        self._sessions: dict[str, ChatV2Session] = {}

    @staticmethod
    def _key(user_id: str) -> str:
        return f"chatv2:{user_id}"

    def get(self, user_id: str) -> ChatV2Session | None:
        key = self._key(user_id)
        session = self._sessions.get(key)
        if session is None:
            return None
        now = time.time()
        if now - session.last_access > SESSION_TTL_SECONDS:
            del self._sessions[key]
            return None
        session.last_access = now
        return session

    def set(
        self,
        user_id: str,
        thread: AgentThread,
        agent: Any = None,
        context_provider: Any = None,
        result_holder: dict | None = None,
    ) -> ChatV2Session:
        key = self._key(user_id)
        session = ChatV2Session(
            thread=thread, agent=agent, context_provider=context_provider,
            result_holder=result_holder,
        )
        self._sessions[key] = session
        return session

    def update_pending_summary(self, user_id: str, summary: str) -> None:
        """Store a pre-built summary for later use during hard compaction."""
        key = self._key(user_id)
        session = self._sessions.get(key)
        if session is not None:
            session.pending_summary = summary

    def consume_pending_summary(self, user_id: str) -> str | None:
        """Retrieve and clear the pending summary."""
        key = self._key(user_id)
        session = self._sessions.get(key)
        if session is not None and session.pending_summary:
            summary = session.pending_summary
            session.pending_summary = None
            return summary
        return None

    def delete(self, user_id: str) -> None:
        self._sessions.pop(self._key(user_id), None)

    def cleanup_expired(self) -> int:
        """Remove expired sessions and return the count removed."""
        now = time.time()
        expired = [
            k
            for k, v in self._sessions.items()
            if now - v.last_access > SESSION_TTL_SECONDS
        ]
        for k in expired:
            del self._sessions[k]
        return len(expired)

    async def save_to_db(
        self, settings: Settings, user_id: str, thread: AgentThread
    ) -> None:
        """Serialize the thread and upsert into dbo.ChatV2Sessions."""
        try:
            state = await thread.serialize()
            key = self._key(user_id)
            state_json = json.dumps(state, ensure_ascii=False, default=str)

            result = await execute_insert(
                settings,
                "UPDATE dbo.ChatV2Sessions SET thread_state = ?, updated_at = GETDATE() "
                "WHERE session_key = ?",
                (state_json, key),
            )
            if result.get("success") and result.get("rows_affected", 0) == 0:
                await execute_insert(
                    settings,
                    "INSERT INTO dbo.ChatV2Sessions "
                    "(session_key, user_id, thread_state, created_at, updated_at) "
                    "VALUES (?, ?, ?, GETDATE(), GETDATE())",
                    (key, user_id, state_json),
                )
            logger.debug("ChatV2 session persisted for %s", key)
        except Exception as e:
            logger.warning("Failed to persist ChatV2 session: %s", e)

    async def load_from_db(
        self, settings: Settings, user_id: str
    ) -> dict[str, Any] | None:
        """Load serialized thread state from DB, or None if not found."""
        try:
            key = self._key(user_id)
            rows = await execute_query(
                settings,
                "SELECT thread_state FROM dbo.ChatV2Sessions WHERE session_key = ?",
                (key,),
            )
            if not rows:
                return None
            return json.loads(rows[0]["thread_state"])
        except Exception as e:
            logger.warning("Failed to load ChatV2 session from DB: %s", e)
            return None

    async def delete_from_db(self, settings: Settings, user_id: str) -> None:
        """Delete persisted session from DB."""
        try:
            key = self._key(user_id)
            await execute_insert(
                settings,
                "DELETE FROM dbo.ChatV2Sessions WHERE session_key = ?",
                (key,),
            )
            logger.debug("ChatV2 session deleted from DB for %s", key)
        except Exception as e:
            logger.warning("Failed to delete ChatV2 session from DB: %s", e)
