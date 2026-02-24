"""Chat V2 agent — single-agent architecture with multi-turn memory."""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import anthropic
from agent_framework import AgentThread, ChatAgent, FunctionInvocationConfiguration
from agent_framework.exceptions import ServiceResponseException

from src.infrastructure.llm.factory import _build_anthropic_client

from src.config.settings import Settings
from src.infrastructure.cache.semantic_cache_v2 import SemanticCacheV2
from src.infrastructure.database.connection import FabricConnectionFactory
from src.infrastructure.database.tools import DelfosTools
from src.services.chat_v2.context import SchemaContextProvider
from src.services.chat_v2.prompts import build_chat_v2_system_prompt
from src.services.chat_v2.session_store import ChatV2SessionStore
from src.services.chat_v2.compaction import compact_thread, should_compact, summarize_messages
from src.services.chat_v2.tools import create_chat_v2_tools, viz_result_ctx

logger = logging.getLogger(__name__)

_session_store = ChatV2SessionStore()
_delfos_tools: DelfosTools | None = None
_semantic_cache: SemanticCacheV2 | None = None


def _get_semantic_cache(settings: Settings) -> SemanticCacheV2 | None:
    """Return singleton SemanticCacheV2 (or None if not configured)."""
    global _semantic_cache  # noqa: PLW0603
    if _semantic_cache is not None:
        return _semantic_cache
    if not settings.aoai_embedding_endpoint or not settings.aoai_embedding_key:
        logger.info("[SEMANTIC CACHE] Disabled — missing AOAI embedding config")
        return None
    _semantic_cache = SemanticCacheV2(
        endpoint=settings.aoai_embedding_endpoint,
        api_key=settings.aoai_embedding_key,
        deployment=settings.aoai_embedding_deployment,
        threshold=settings.semantic_cache_threshold,
        max_size=settings.semantic_cache_max_size,
        ttl_seconds=settings.semantic_cache_ttl,
    )
    logger.info(
        "[SEMANTIC CACHE] Initialized (threshold=%.2f, max_size=%d, ttl=%ds)",
        settings.semantic_cache_threshold,
        settings.semantic_cache_max_size,
        settings.semantic_cache_ttl,
    )
    return _semantic_cache

# Retry config for 429 rate-limit errors
_RATE_LIMIT_MAX_RETRIES = 4
_RATE_LIMIT_DEFAULT_WAIT = 10


def _get_retry_after(exc: Exception) -> float:
    """Extract retry-after seconds from a rate-limit exception chain."""
    cause = exc.__cause__ if isinstance(exc, ServiceResponseException) else exc
    # Anthropic rate limit
    if isinstance(cause, anthropic.RateLimitError) and hasattr(cause, "response"):
        retry_sec = cause.response.headers.get("retry-after")
        if retry_sec:
            try:
                return float(retry_sec)
            except (TypeError, ValueError):
                pass
    return _RATE_LIMIT_DEFAULT_WAIT


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if an exception is a 429 rate-limit error."""
    if isinstance(exc, anthropic.RateLimitError):
        return True
    if isinstance(exc, ServiceResponseException):
        cause = exc.__cause__
        if isinstance(cause, anthropic.RateLimitError):
            return True
        if cause and "rate" in str(cause).lower():
            return True
    return False


import re

_GREETING_RE = re.compile(
    r"^(hola|hi|hello|buenos?\s*d[ií]as?|buenas?\s*(tardes?|noches?)|hey|saludos|qu[eé]\s*tal|gracias|chao|adi[oó]s)\b",
    re.IGNORECASE,
)


def _resolve_tool_choice(message: str) -> str:
    """Return tool_choice for the given message.

    ``"required"`` for data questions (forces the tool call on iteration 0).
    ``"auto"`` for greetings/short non-questions (lets LLM respond directly).

    The infinite-loop problem is solved by ``max_iterations=1`` on the client's
    ``FunctionInvocationConfiguration``: after 1 iteration the framework's
    failsafe switches to ``tool_choice="none"`` for the final text response.
    """
    msg = message.strip()
    if len(msg) < 30 and _GREETING_RE.search(msg):
        return "auto"
    if len(msg) < 15 and "?" not in msg:
        return "auto"
    return "required"


def _get_delfos_tools(settings: Settings) -> DelfosTools:
    """Return singleton DelfosTools."""
    global _delfos_tools  # noqa: PLW0603
    if _delfos_tools is None:
        wh_factory = FabricConnectionFactory(settings.wh_server, settings.wh_database)
        db_factory = FabricConnectionFactory(settings.db_server, settings.db_database)
        _delfos_tools = DelfosTools(
            wh_factory=wh_factory,
            db_factory=db_factory,
            wh_schema=settings.wh_schema,
            db_schema=settings.db_schema,
            workspace_id=settings.powerbi_workspace_id,
            report_id=settings.powerbi_report_id,
        )
    return _delfos_tools


async def _save_session_bg(
    settings: Settings, user_id: str, thread: AgentThread
) -> None:
    """Persist chat session in background (fire-and-forget)."""
    try:
        await _session_store.save_to_db(settings, user_id, thread)
    except Exception:
        logger.warning("Background session save failed for %s", user_id, exc_info=True)


class ChatV2Agent:
    """Single-agent chat with multi-turn memory and @ai_function tools."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _create_agent_with_provider(
        self, context_provider: SchemaContextProvider
    ) -> tuple[ChatAgent, dict]:
        """Create a ChatAgent wired to the given context provider.

        Returns (agent, result_holder) where result_holder is a mutable dict
        shared with the tools to capture viz/clarification results.
        """
        client = _build_anthropic_client(
            self.settings, self.settings.chat_v2_agent_model
        )
        # 1 tool call (required → one of the tools), then failsafe(none) → text
        client.function_invocation_configuration = FunctionInvocationConfiguration(
            max_iterations=1
        )
        tools, result_holder = create_chat_v2_tools(
            _get_delfos_tools(self.settings), self.settings
        )
        agent = client.create_agent(
            name="DelfosChat",
            instructions=build_chat_v2_system_prompt(),
            tools=tools,
            context_providers=[context_provider],
            temperature=self.settings.chat_v2_temperature,
            max_tokens=self.settings.chat_v2_max_tokens,
        )
        return agent, result_holder

    async def _prepare_agent_and_thread(
        self,
        user_id: str,
    ) -> tuple[ChatAgent, AgentThread, dict]:
        """Resolve (agent, thread, result_holder) from memory, database, or create new."""
        session = _session_store.get(user_id)
        if session is not None and session.agent is not None:
            return session.agent, session.thread, session.result_holder or {}

        context_provider = SchemaContextProvider()
        agent, result_holder = self._create_agent_with_provider(context_provider)

        saved_state = await _session_store.load_from_db(self.settings, user_id)
        if saved_state is not None:
            logger.info("Restoring ChatV2 session from DB for %s", user_id)
            thread = await agent.deserialize_thread(saved_state)
            # Old Azure OpenAI sessions have a service_thread_id that is
            # incompatible with AnthropicClient (stateless).  Discard and
            # start fresh so the framework doesn't crash.
            if thread.service_thread_id is not None:
                logger.warning(
                    "Discarding incompatible service-managed thread for %s",
                    user_id,
                )
                thread = agent.get_new_thread()
        else:
            thread = agent.get_new_thread()

        _session_store.set(
            user_id, thread, agent=agent, context_provider=context_provider,
            result_holder=result_holder,
        )
        return agent, thread, result_holder

    async def _maybe_compact(self, user_id: str, thread: AgentThread) -> None:
        """Check message count and compact if needed (hard) or prepare (soft)."""
        store = thread.message_store
        if store is None:
            return
        message_count = len(store.messages)
        action = should_compact(self.settings, message_count)

        if action == "hard":
            pre_built = _session_store.consume_pending_summary(user_id)
            logger.info(
                "[COMPACTION] Hard threshold (%d msgs). Pre-built: %s",
                message_count, "YES" if pre_built else "NO",
            )
            await compact_thread(self.settings, thread, pre_built_summary=pre_built)
        elif action == "soft":
            session = _session_store.get(user_id)
            if session and (session.bg_summary_task is None or session.bg_summary_task.done()):
                logger.info(
                    "[COMPACTION] Soft threshold (%d msgs). Starting background summary.",
                    message_count,
                )
                session.bg_summary_task = asyncio.create_task(
                    self._build_summary_bg(user_id, list(store.messages))
                )

    async def _build_summary_bg(self, user_id: str, messages: list) -> None:
        """Build a summary in background and store for later use."""
        try:
            keep = self.settings.chat_v2_compaction_keep_recent
            msgs_to_summarize = messages[:-keep] if keep > 0 else messages
            summary = await summarize_messages(self.settings, msgs_to_summarize)
            _session_store.update_pending_summary(user_id, summary)
            logger.info(
                "[COMPACTION] Background summary ready for %s (%d chars)",
                user_id, len(summary),
            )
        except Exception:
            logger.warning("Background summary failed for %s", user_id, exc_info=True)

    async def chat(self, user_id: str, message: str) -> str:
        """Send a message and return the full response."""
        # --- Semantic cache lookup (only for data queries) ---
        cache = _get_semantic_cache(self.settings)
        embedding: list[float] | None = None
        is_data_query = _resolve_tool_choice(message) == "required"

        if cache and is_data_query:
            try:
                embedding = cache.embed(message)
                cached_result, score = cache.search(embedding)
                if cached_result is not None:
                    logger.info(
                        "[SEMANTIC CACHE] HIT (score=%.3f) for: %s", score, message[:60],
                    )
                    return cached_result.get("text", "")
            except Exception:
                logger.warning("Semantic cache lookup failed", exc_info=True)

        agent, thread, result_holder = await self._prepare_agent_and_thread(user_id)
        await self._maybe_compact(user_id, thread)

        # Determine the cache key: original question from Turn 1 if this is
        # a clarification response, otherwise the current message.
        prev_clarif = result_holder.get("clarification")
        if prev_clarif and prev_clarif.get("clarification"):
            cache_key_question = result_holder.get("_pending_cache_question", message)
        else:
            cache_key_question = message

        last_exc: Exception | None = None
        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                tc = _resolve_tool_choice(message)
                response = await agent.run(message, thread=thread, tool_choice=tc)
                break
            except Exception as exc:
                if _is_rate_limit_error(exc) and attempt < _RATE_LIMIT_MAX_RETRIES:
                    base_wait = _get_retry_after(exc)
                    wait = base_wait * (2 ** attempt)
                    logger.warning(
                        "Rate-limited (429) on attempt %d, waiting %.1fs before retry",
                        attempt + 1, wait,
                    )
                    await asyncio.sleep(wait)
                    last_exc = exc
                    continue
                logger.exception("agent.run() failed for user=%s", user_id)
                raise
        else:
            logger.error("All %d rate-limit retries exhausted", _RATE_LIMIT_MAX_RETRIES)
            raise last_exc  # type: ignore[misc]

        response_text = str(response.text)

        # --- Semantic cache store (only if we got viz data = real data query) ---
        if cache and is_data_query:
            viz = result_holder.get("viz") or viz_result_ctx.get()
            if viz and viz.get("visualization"):
                try:
                    cache_embedding = cache.embed(cache_key_question)
                    cache.store(
                        key=cache_key_question,
                        question=cache_key_question,
                        result={"text": response_text, "viz_data": viz},
                        embedding=cache_embedding,
                    )
                except Exception:
                    logger.warning("Semantic cache store failed", exc_info=True)
            # If clarification was requested, save original question for next turn
            clarif = result_holder.get("clarification")
            if clarif and clarif.get("clarification"):
                result_holder["_pending_cache_question"] = cache_key_question

        # Persist session in background
        asyncio.create_task(_save_session_bg(self.settings, user_id, thread))
        return response_text

    async def chat_stream(
        self, user_id: str, message: str
    ) -> AsyncIterator[str]:
        """Stream response tokens, persisting the thread after completion.

        After all text chunks, yields sentinel-prefixed chunks for:
        - ``__CLARIFICATION__`` — when the agent asked for temporal clarification
        - ``__VIZ_DATA__`` — when execute_and_visualize produced visualization data

        The SSE router splits on these sentinels to emit dedicated events,
        guaranteeing the frontend receives structured data regardless of LLM text.
        """
        _VIZ_SENTINEL = "__VIZ_DATA__"
        _CLARIFICATION_SENTINEL = "__CLARIFICATION__"

        # --- Semantic cache lookup (only for data queries) ---
        cache = _get_semantic_cache(self.settings)
        embedding: list[float] | None = None
        is_data_query = _resolve_tool_choice(message) == "required"

        if cache and is_data_query:
            try:
                embedding = cache.embed(message)
                cached_result, score = cache.search(embedding)
                if cached_result is not None:
                    logger.info(
                        "[SEMANTIC CACHE] STREAM HIT (score=%.3f) for: %s",
                        score, message[:60],
                    )
                    # Yield cached text
                    cached_text = cached_result.get("text", "")
                    if cached_text:
                        yield cached_text
                    # Yield cached viz data
                    cached_viz = cached_result.get("viz_data")
                    if cached_viz and cached_viz.get("visualization"):
                        yield _VIZ_SENTINEL + json.dumps(
                            cached_viz, ensure_ascii=False, default=str,
                        )
                    return
            except Exception:
                logger.warning("Semantic cache stream lookup failed", exc_info=True)

        agent, thread, result_holder = await self._prepare_agent_and_thread(user_id)
        await self._maybe_compact(user_id, thread)

        # If previous turn was a clarification, the cache key should be the
        # ORIGINAL question (Turn 1), not this clarification answer (Turn 2).
        prev_clarif = result_holder.get("clarification")
        if prev_clarif and prev_clarif.get("clarification"):
            # Recover the original question saved during Turn 1
            cache_key_question = result_holder.get("_pending_cache_question", message)
            questions = prev_clarif.get("questions", [])
            q_labels = ", ".join(q.get("id", "") for q in questions) if questions else "periodo"
            message = (
                f'[RESPUESTA A CLARIFICACIÓN PREVIA ({q_labels})] '
                f'El usuario respondió: "{message}". '
                f'Usa estas respuestas junto con la pregunta original del historial '
                f'para escribir el SQL y llama execute_and_visualize.'
            )
            logger.info("[AGENT] Wrapped clarification response: %s", message[:120])
        else:
            cache_key_question = message

        # Reset holders for this request
        result_holder["viz"] = None
        result_holder["clarification"] = None
        result_holder["_pending_cache_question"] = None
        viz_result_ctx.set(None)

        collected_text: list[str] = []
        last_exc: Exception | None = None
        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                tc = _resolve_tool_choice(message)
                async for chunk in agent.run_stream(message, thread=thread, tool_choice=tc):
                    if chunk.text:
                        collected_text.append(chunk.text)
                        yield chunk.text
                last_exc = None
                break
            except Exception as exc:
                if _is_rate_limit_error(exc) and attempt < _RATE_LIMIT_MAX_RETRIES:
                    base_wait = _get_retry_after(exc)
                    wait = base_wait * (2 ** attempt)
                    logger.warning(
                        "Stream rate-limited (429) on attempt %d, waiting %.1fs",
                        attempt + 1, wait,
                    )
                    await asyncio.sleep(wait)
                    last_exc = exc
                    collected_text.clear()
                    continue
                logger.exception("agent.run_stream() failed for user=%s", user_id)
                raise
        if last_exc is not None:
            logger.error("All %d stream rate-limit retries exhausted", _RATE_LIMIT_MAX_RETRIES)
            raise last_exc

        # Yield clarification if the agent asked for it
        clarif = result_holder.get("clarification")
        if clarif and clarif.get("clarification"):
            # Save the original question so Turn 2 can use it as cache key
            result_holder["_pending_cache_question"] = cache_key_question
            yield _CLARIFICATION_SENTINEL + json.dumps(clarif, ensure_ascii=False)

        # Yield viz data (primary: mutable holder, fallback: ContextVar)
        viz = result_holder.get("viz") or viz_result_ctx.get()
        if viz and viz.get("visualization"):
            yield _VIZ_SENTINEL + json.dumps(viz, ensure_ascii=False, default=str)

        # --- Semantic cache store (only if we got viz data = real data query) ---
        if cache and is_data_query and viz and viz.get("visualization"):
            try:
                full_text = "".join(collected_text)
                # cache_key_question is the ORIGINAL question (from Turn 1 if
                # clarification, or the current message otherwise).
                cache_embedding = cache.embed(cache_key_question)
                cache.store(
                    key=cache_key_question,
                    question=cache_key_question,
                    result={"text": full_text, "viz_data": viz},
                    embedding=cache_embedding,
                )
                logger.info(
                    "[SEMANTIC CACHE] Stored with key: %s", cache_key_question[:60],
                )
            except Exception:
                logger.warning("Semantic cache stream store failed", exc_info=True)

        # Persist session in background
        asyncio.create_task(_save_session_bg(self.settings, user_id, thread))

    @staticmethod
    async def clear_session(settings: Settings, user_id: str) -> None:
        """Remove the session from memory and database."""
        _session_store.delete(user_id)
        await _session_store.delete_from_db(settings, user_id)
