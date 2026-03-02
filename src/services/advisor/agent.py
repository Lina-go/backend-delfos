"""Advisor agent."""

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

import anthropic
import httpx
from agent_framework import AgentThread, ChatAgent
from agent_framework.azure import AzureOpenAIResponsesClient
from agent_framework.exceptions import ServiceResponseException
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from openai import RateLimitError as OpenAIRateLimitError

from src.config.settings import Settings
from src.infrastructure.llm.factory import _build_anthropic_client, is_anthropic_model
from src.services.advisor.context import ReportContextProvider
from src.services.advisor.prompts import build_advisor_system_prompt
from src.services.advisor.session_store import AdvisorSessionStore
from src.services.advisor.tools import create_advisor_tools

logger = logging.getLogger(__name__)

_session_store = AdvisorSessionStore()
_credential: DefaultAzureCredential | ClientSecretCredential | None = None


def warmup_credential(settings: Settings) -> None:
    """Pre-initialize the Azure credential singleton. Called once at startup."""
    _get_credential(settings)


def _get_advisor_cache(settings: Settings) -> Any:
    """Return the shared SemanticCacheV2 singleton (same one used by chat_v2)."""
    try:
        from src.services.chat_v2.agent import _get_semantic_cache
        return _get_semantic_cache(settings)
    except Exception:
        return None

# Retry config for Azure OpenAI 429 rate-limit errors
_RATE_LIMIT_MAX_RETRIES = 2
_RATE_LIMIT_DEFAULT_WAIT = 20  # seconds, if no retry-after header

# HTTP timeout for the Azure OpenAI client (default SDK timeout is 600s — way too long)
_ADVISOR_HTTP_TIMEOUT = httpx.Timeout(120.0, connect=10.0)


def _get_retry_after(exc: Exception) -> float:
    """Extract retry-after seconds from a rate-limit exception chain."""
    cause = exc.__cause__ if isinstance(exc, ServiceResponseException) else exc
    # OpenAI rate limit
    if isinstance(cause, OpenAIRateLimitError) and hasattr(cause, "response"):
        headers = cause.response.headers
        retry_ms = headers.get("retry-after-ms")
        if retry_ms:
            try:
                return float(retry_ms) / 1000
            except (TypeError, ValueError):
                pass
        retry_sec = headers.get("retry-after")
        if retry_sec:
            try:
                return float(retry_sec)
            except (TypeError, ValueError):
                pass
    # Anthropic rate limit
    if isinstance(cause, anthropic.RateLimitError) and hasattr(cause, "response"):
        retry_sec = cause.response.headers.get("retry-after")
        if retry_sec:
            try:
                return float(retry_sec)
            except (TypeError, ValueError):
                pass
    return _RATE_LIMIT_DEFAULT_WAIT


_RATE_LIMIT_ERRORS = (OpenAIRateLimitError, anthropic.RateLimitError)


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if an exception is a 429 rate-limit error (OpenAI or Anthropic)."""
    if isinstance(exc, _RATE_LIMIT_ERRORS):
        return True
    if isinstance(exc, ServiceResponseException):
        return isinstance(exc.__cause__, _RATE_LIMIT_ERRORS)
    return False


def _parse_insights(raw_text: str) -> list[dict[str, str]]:
    """Extract a JSON list of insights from LLM text, with fallback."""
    import json
    import re

    # Try to find a JSON array in the response
    match = re.search(r"\[.*\]", raw_text, re.DOTALL)
    if match:
        try:
            items = json.loads(match.group())
            if isinstance(items, list):
                return [
                    {
                        "title": str(it.get("title", "Insight")),
                        "description": str(it.get("description", "")),
                        "severity": str(it.get("severity", "MODERADO")),
                    }
                    for it in items[:3]
                    if isinstance(it, dict)
                ]
        except json.JSONDecodeError:
            pass

    # Fallback: wrap raw text as a single insight
    logger.warning("[ADVISOR] Could not parse insights JSON, using fallback")
    return [{"title": "Resumen del informe", "description": raw_text[:500], "severity": "MODERADO"}]


def _get_delfos_tools(settings: Settings):
    """Return the shared DelfosTools singleton (pre-warmed at startup by app.py)."""
    from src.services.chat_v2.agent import _get_delfos_tools as _shared_get_delfos_tools
    return _shared_get_delfos_tools(settings)


def _get_credential(
    settings: Settings,
) -> DefaultAzureCredential | ClientSecretCredential:
    """Return singleton sync Azure credential (separate from the async one in llm/factory)."""
    global _credential  # noqa: PLW0603
    if _credential is not None:
        return _credential

    if settings.use_service_principal:
        _credential = ClientSecretCredential(
            tenant_id=settings.azure_tenant_id,
            client_id=settings.azure_client_id,
            client_secret=settings.azure_client_secret,
        )
    else:
        _credential = DefaultAzureCredential(
            exclude_visual_studio_code_credential=True,
            exclude_powershell_credential=True,
            exclude_developer_cli_credential=True,
        )
    return _credential


async def _save_session_bg(
    settings: Settings, user_id: str, informe_id: str, thread: AgentThread
) -> None:
    """Persist advisor session in background (fire-and-forget)."""
    try:
        await _session_store.save_to_db(settings, user_id, informe_id, thread)
    except Exception:
        logger.warning("Background session save failed for %s:%s", user_id, informe_id, exc_info=True)


class AdvisorAgent:
    """Financial advisor agent with multi-turn sessions per (user_id, informe_id)."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _create_agent_with_provider(
        self, context_provider: ReportContextProvider
    ) -> ChatAgent:
        """Create a ChatAgent wired to the given context provider."""
        model = self.settings.advisor_agent_model
        tools = create_advisor_tools(_get_delfos_tools(self.settings))
        instructions = build_advisor_system_prompt()

        if is_anthropic_model(model):
            client = _build_anthropic_client(self.settings, model)
            return client.create_agent(
                name="DelfosAdvisor",
                instructions=instructions,
                tools=tools,
                context_providers=[context_provider],
                temperature=self.settings.advisor_temperature,
                max_tokens=self.settings.advisor_max_tokens,
            )

        # Azure OpenAI path (default)
        client = AzureOpenAIResponsesClient(
            endpoint=self.settings.azure_ai_project_endpoint,
            deployment_name=model,
            credential=_get_credential(self.settings),
        )
        # Patch HTTP timeout on the internal OpenAI client (SDK default is 600s)
        if hasattr(client, "client") and client.client is not None:
            client.client.timeout = _ADVISOR_HTTP_TIMEOUT
        return client.create_agent(
            name="DelfosAdvisor",
            instructions=instructions,
            tools=tools,
            context_providers=[context_provider],
            temperature=self.settings.advisor_temperature,
            max_tokens=self.settings.advisor_max_tokens,
            allow_multiple_tool_calls=True,
        )

    async def _prepare_agent_and_thread(
        self,
        user_id: str,
        informe_id: str,
        report_context: dict[str, Any],
    ) -> tuple[ChatAgent, AgentThread]:
        """Resolve (agent, thread) from memory, database, or create new."""
        session = _session_store.get(user_id, informe_id)
        if session is not None and session.agent is not None:
            session.context_provider.update_context(report_context)
            return session.agent, session.thread

        context_provider = ReportContextProvider(report_context)
        agent = self._create_agent_with_provider(context_provider)

        saved_state = await _session_store.load_from_db(
            self.settings, user_id, informe_id
        )
        if saved_state is not None:
            logger.info("Restoring advisor session from DB for %s:%s", user_id, informe_id)
            try:
                thread = await agent.deserialize_thread(saved_state)
                # Guard: discard service-managed threads when switching to Anthropic
                if is_anthropic_model(self.settings.advisor_agent_model):
                    if getattr(thread, "service_thread_id", None) is not None:
                        logger.warning(
                            "Discarding incompatible OpenAI thread for %s:%s (switched to Anthropic)",
                            user_id, informe_id,
                        )
                        thread = agent.get_new_thread()
            except Exception as e:
                logger.warning(
                    "Failed to deserialize advisor thread for %s:%s: %s — starting fresh",
                    user_id, informe_id, e,
                )
                thread = agent.get_new_thread()
        else:
            thread = agent.get_new_thread()

        _session_store.set(
            user_id, informe_id, thread,
            agent=agent, context_provider=context_provider,
        )
        return agent, thread

    def _get_turn_count(self, user_id: str, informe_id: str) -> int:
        """Return the turn count from the session store."""
        return _session_store.get_turn_count(user_id, informe_id)

    def _maybe_reset_thread(
        self, user_id: str, informe_id: str, agent: ChatAgent
    ) -> AgentThread | None:
        """Reset thread if turn count exceeds threshold."""
        turn_count = self._get_turn_count(user_id, informe_id)
        if turn_count >= 15:
            logger.info(
                "[ADVISOR COMPACT] Resetting thread after %d turns for %s:%s",
                turn_count, user_id, informe_id,
            )
            new_thread = agent.get_new_thread()
            session = _session_store.get(user_id, informe_id)
            if session:
                session.thread = new_thread
                session.turn_count = 0
            return new_thread
        return None

    async def chat(
        self,
        user_id: str,
        informe_id: str,
        message: str,
        report_context: dict[str, Any],
    ) -> str:
        """Send a message and return the full response."""
        t0 = time.time()
        agent, thread = await self._prepare_agent_and_thread(
            user_id, informe_id, report_context
        )
        turn_count = self._get_turn_count(user_id, informe_id)
        logger.info(
            "[ADVISOR TIMING] Session prep: %.2fs (turns=%d)",
            time.time() - t0, turn_count,
        )
        new_thread = self._maybe_reset_thread(user_id, informe_id, agent)
        if new_thread:
            thread = new_thread
        last_exc: Exception | None = None
        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                response = await agent.run(message, thread=thread)
                break
            except Exception as exc:
                if _is_rate_limit_error(exc) and attempt < _RATE_LIMIT_MAX_RETRIES:
                    wait = _get_retry_after(exc)
                    logger.warning(
                        "Rate-limited (429) on attempt %d, waiting %.1fs before retry",
                        attempt + 1, wait,
                    )
                    await asyncio.sleep(wait)
                    last_exc = exc
                    continue
                logger.exception("agent.run() failed for user=%s informe=%s", user_id, informe_id)
                raise
        else:
            logger.error("All %d rate-limit retries exhausted", _RATE_LIMIT_MAX_RETRIES)
            raise last_exc  # type: ignore[misc]
        _session_store.increment_turn(user_id, informe_id)
        # Persist session in background — don't block the response
        asyncio.create_task(
            _save_session_bg(self.settings, user_id, informe_id, thread)
        )
        return str(response.text)

    async def chat_stream(
        self,
        user_id: str,
        informe_id: str,
        message: str,
        report_context: dict[str, Any],
    ) -> AsyncIterator[str]:
        """Stream response tokens, persisting the thread after completion."""
        t0 = time.time()
        agent, thread = await self._prepare_agent_and_thread(
            user_id, informe_id, report_context
        )
        turn_count = self._get_turn_count(user_id, informe_id)
        logger.info(
            "[ADVISOR TIMING] Session prep: %.2fs (turns=%d)",
            time.time() - t0, turn_count,
        )
        new_thread = self._maybe_reset_thread(user_id, informe_id, agent)
        if new_thread:
            thread = new_thread
            turn_count = 0

        # --- Semantic cache lookup (only for first message in session) ---
        cache = _get_advisor_cache(self.settings)
        cache_embedding = None
        if cache and turn_count == 0:
            try:
                cache_embedding = cache.embed(message)
                cached, score = cache.search(cache_embedding, query_text=message)
                if cached:
                    logger.info("[ADVISOR CACHE] HIT score=%.3f for '%s'", score, message[:80])
                    yield cached["result"]["text"]
                    return
            except Exception:
                logger.debug("[ADVISOR CACHE] Lookup failed", exc_info=True)

        t_llm = time.time()
        t_first_token: float | None = None
        full_response_parts: list[str] = []
        last_exc: Exception | None = None
        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                async for chunk in agent.run_stream(message, thread=thread):
                    if chunk.text:
                        if t_first_token is None:
                            t_first_token = time.time() - t_llm
                            logger.info("[ADVISOR TIMING] First token: %.2fs", t_first_token)
                        full_response_parts.append(chunk.text)
                        yield chunk.text
                last_exc = None  # success
                break
            except Exception as exc:
                if _is_rate_limit_error(exc) and attempt < _RATE_LIMIT_MAX_RETRIES:
                    wait = _get_retry_after(exc)
                    logger.warning(
                        "Stream rate-limited (429) on attempt %d, waiting %.1fs",
                        attempt + 1, wait,
                    )
                    await asyncio.sleep(wait)
                    last_exc = exc
                    continue
                logger.exception("agent.run_stream() failed for user=%s informe=%s", user_id, informe_id)
                raise
        if last_exc is not None:
            logger.error("All %d stream rate-limit retries exhausted", _RATE_LIMIT_MAX_RETRIES)
            raise last_exc

        logger.info("[ADVISOR TIMING] Total stream: %.2fs", time.time() - t_llm)

        _session_store.increment_turn(user_id, informe_id)

        # --- Semantic cache store (only first turn, non-contextual answers) ---
        if cache and cache_embedding and turn_count == 0:
            try:
                full_text = "".join(full_response_parts)
                if full_text.strip():
                    cache.store(
                        key=f"advisor:{informe_id}:{message[:100]}",
                        question=message,
                        result={"text": full_text},
                        embedding=cache_embedding,
                    )
            except Exception:
                logger.debug("[ADVISOR CACHE] Store failed", exc_info=True)

        # Persist session in background — don't block the stream
        asyncio.create_task(
            _save_session_bg(self.settings, user_id, informe_id, thread)
        )

    async def generate_proactive_insights(
        self, user_id: str, informe_id: str, report_context: dict[str, Any]
    ) -> list[dict[str, str]]:
        """Generate top-3 proactive insights on a disposable thread (not persisted)."""
        context_provider = ReportContextProvider(report_context)
        agent = self._create_agent_with_provider(context_provider)
        thread = agent.get_new_thread()
        prompt = (
            "Analiza los graficos del informe actual usando los datos disponibles en el contexto. "
            "Identifica los TOP 3 hallazgos financieros mas relevantes y accionables.\n\n"
            "Para cada hallazgo responde con:\n"
            "1. title: Titulo corto (max 10 palabras)\n"
            "2. description: Descripcion de 2-3 oraciones con datos concretos (valores, porcentajes, tendencias)\n"
            "3. severity: Nivel de relevancia — uno de: CRITICO, ALTO, MODERADO\n\n"
            "Prioriza: cambios significativos, anomalias, tendencias que requieren atencion.\n\n"
            "Responde UNICAMENTE con un JSON array de 3 objetos, sin texto adicional:\n"
            '[{"title": "...", "description": "...", "severity": "..."}, ...]'
        )
        last_exc: Exception | None = None
        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                response = await agent.run(prompt, thread=thread)
                return _parse_insights(str(response.text))
            except Exception as exc:
                if _is_rate_limit_error(exc) and attempt < _RATE_LIMIT_MAX_RETRIES:
                    wait = _get_retry_after(exc)
                    logger.warning(
                        "Insights rate-limited (429) on attempt %d, waiting %.1fs",
                        attempt + 1, wait,
                    )
                    await asyncio.sleep(wait)
                    last_exc = exc
                    continue
                raise
        raise last_exc  # type: ignore[misc]

    @staticmethod
    async def clear_session(settings: Settings, user_id: str, informe_id: str) -> None:
        """Remove the session from memory and database."""
        _session_store.delete(user_id, informe_id)
        await _session_store.delete_from_db(settings, user_id, informe_id)
