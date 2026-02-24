"""Advisor agent — financial analyst for informe chat."""

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from agent_framework import AgentThread, ChatAgent, HostedCodeInterpreterTool
from agent_framework.azure import AzureOpenAIResponsesClient
from agent_framework.exceptions import ServiceResponseException
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from openai import RateLimitError

from src.config.settings import Settings
from src.infrastructure.database.connection import FabricConnectionFactory
from src.infrastructure.database.tools import DelfosTools
from src.services.advisor.context import ReportContextProvider
from src.services.advisor.prompts import build_advisor_system_prompt
from src.services.advisor.session_store import AdvisorSessionStore
from src.services.advisor.tools import create_advisor_tools

logger = logging.getLogger(__name__)

_session_store = AdvisorSessionStore()
_delfos_tools: DelfosTools | None = None
_credential: DefaultAzureCredential | ClientSecretCredential | None = None

# Retry config for Azure OpenAI 429 rate-limit errors
_RATE_LIMIT_MAX_RETRIES = 2
_RATE_LIMIT_DEFAULT_WAIT = 20  # seconds, if no retry-after header


def _get_retry_after(exc: Exception) -> float:
    """Extract retry-after seconds from a rate-limit exception chain."""
    cause = exc.__cause__ if isinstance(exc, ServiceResponseException) else exc
    if isinstance(cause, RateLimitError) and hasattr(cause, "response"):
        headers = cause.response.headers
        # Try retry-after-ms first (more precise), then retry-after
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
    return _RATE_LIMIT_DEFAULT_WAIT


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if an exception is a 429 rate-limit error."""
    if isinstance(exc, RateLimitError):
        return True
    if isinstance(exc, ServiceResponseException):
        return isinstance(exc.__cause__, RateLimitError)
    return False


def _get_delfos_tools(settings: Settings) -> DelfosTools:
    """Return singleton ``DelfosTools``."""
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
        """Create a ``ChatAgent`` wired to the given context provider."""
        client = AzureOpenAIResponsesClient(
            endpoint=self.settings.azure_ai_project_endpoint,
            deployment_name=self.settings.advisor_agent_model,
            credential=_get_credential(self.settings),
        )
        return client.create_agent(
            name="DelfosAdvisor",
            instructions=build_advisor_system_prompt(),
            tools=[
                HostedCodeInterpreterTool(),
                *create_advisor_tools(_get_delfos_tools(self.settings)),
            ],
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
            thread = await agent.deserialize_thread(saved_state)
        else:
            thread = agent.get_new_thread()

        _session_store.set(
            user_id, informe_id, thread,
            agent=agent, context_provider=context_provider,
        )
        return agent, thread

    async def chat(
        self,
        user_id: str,
        informe_id: str,
        message: str,
        report_context: dict[str, Any],
    ) -> str:
        """Send a message and return the full response."""
        agent, thread = await self._prepare_agent_and_thread(
            user_id, informe_id, report_context
        )
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
        agent, thread = await self._prepare_agent_and_thread(
            user_id, informe_id, report_context
        )
        last_exc: Exception | None = None
        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                async for chunk in agent.run_stream(message, thread=thread):
                    if chunk.text:
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
        # Persist session in background — don't block the stream
        asyncio.create_task(
            _save_session_bg(self.settings, user_id, informe_id, thread)
        )

    async def generate_proactive_insights(
        self, user_id: str, informe_id: str, report_context: dict[str, Any]
    ) -> str:
        """Generate proactive insights on a disposable thread (not persisted)."""
        context_provider = ReportContextProvider(report_context)
        agent = self._create_agent_with_provider(context_provider)
        thread = agent.get_new_thread()
        prompt = (
            "Analiza los graficos del informe actual usando los datos disponibles en el contexto. "
            "Para cada grafico: resume que muestra, identifica valores clave, tendencias visibles "
            "y cualquier patron notable en los data_points. "
            "Organiza el resumen por labels/pestañas del informe. "
            "Al final, sugiere 2-3 preguntas de profundizacion que podrias responder usando herramientas analiticas."
        )
        last_exc: Exception | None = None
        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                response = await agent.run(prompt, thread=thread)
                return str(response.text)
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
