"""Session memory compaction for Chat V2 agent threads.

Adapted from Anthropic's "Instant Session Memory Compaction" cookbook pattern
for use with the Microsoft Agent Framework.

Two compaction modes:
- Background (soft): Pre-builds a summary when message count crosses soft threshold
- Synchronous (hard): Forces compaction before next LLM call when hard threshold crossed
"""

import logging
from typing import Any

from agent_framework import ChatMessage
from agent_framework._threads import ChatMessageStore

from src.config.settings import Settings
from src.orchestrator.handlers._llm_helper import run_handler_agent

logger = logging.getLogger(__name__)

COMPACTION_SYSTEM_PROMPT = """\
Eres un asistente especializado en resumir conversaciones sobre datos financieros \
del sistema financiero colombiano (Superintendencia Financiera de Colombia).

Tu tarea es crear un resumen estructurado que preserve toda la informacion \
necesaria para continuar la conversacion sin perder contexto.

Formato del resumen:

<resumen_sesion>
## Preguntas realizadas
- [Lista de preguntas de datos con sus respuestas resumidas]

## Clarificaciones resueltas
- [Que clarificaciones se pidieron y que respondio el usuario]

## Entidades y metricas discutidas
- Entidades: [nombres exactos de bancos/entidades]
- Metricas: [saldo de cartera, tasas de interes, montos desembolsados, etc.]
- Tablas consultadas: [gold.distribucion_cartera, etc.]

## Preferencias del usuario
- [Top N preferido, periodos usados, granularidad, tipo de graficas]

## Ultimo contexto activo
- Ultima pregunta: [la pregunta mas reciente]
- Ultimo resultado: [breve descripcion del ultimo resultado/grafica]
- Estado pendiente: [si hay clarificacion pendiente o flujo incompleto]
</resumen_sesion>

Reglas:
1. Escribe TODO en espanol
2. Preserva nombres exactos de entidades financieras
3. Preserva periodos exactos mencionados
4. NO inventes informacion que no este en la conversacion
5. Se conciso pero completo
6. Si hubo errores SQL o reintentos, NO los incluyas -- solo el resultado final
"""


def _format_messages_for_summary(messages: list[ChatMessage]) -> str:
    """Convert ChatMessage list to a readable transcript for the summarizer."""
    lines: list[str] = []
    for msg in messages:
        role = msg.role.value if hasattr(msg.role, "value") else str(msg.role)
        text = msg.text or ""

        if not text.strip():
            # Check for function call content
            func_names = []
            for c in msg.contents or []:
                name = getattr(c, "function_name", None) or getattr(c, "name", None)
                if name:
                    func_names.append(name)
            if func_names:
                text = f"[Llamada a herramienta: {', '.join(func_names)}]"
            else:
                continue

        # Truncate long tool results (SQL data, viz JSON)
        if role == "tool" and len(text) > 500:
            text = text[:500] + "... [truncado]"

        role_label = {"user": "Usuario", "assistant": "Asistente", "tool": "Herramienta"}.get(
            role, role
        )
        lines.append(f"{role_label}: {text}")

    return "\n\n".join(lines)


async def summarize_messages(settings: Settings, messages: list[ChatMessage]) -> str:
    """Summarize a list of ChatMessages using a fast/cheap model."""
    transcript = _format_messages_for_summary(messages)
    return await run_handler_agent(
        settings,
        name="SessionCompactor",
        instructions=COMPACTION_SYSTEM_PROMPT,
        message=f"Resume la siguiente conversacion:\n\n{transcript}",
        model=settings.chat_v2_compaction_model,
        max_tokens=1024,
        temperature=0.0,
    )


async def compact_thread(
    settings: Settings,
    thread: Any,
    *,
    pre_built_summary: str | None = None,
) -> bool:
    """Compact a thread's message history by replacing old messages with a summary.

    Returns True if compaction was performed.
    """
    store = thread.message_store
    if store is None:
        return False

    messages = store.messages
    keep_recent = settings.chat_v2_compaction_keep_recent

    if len(messages) <= keep_recent + 1:
        return False

    messages_to_summarize = messages[:-keep_recent] if keep_recent > 0 else messages
    recent_messages = messages[-keep_recent:] if keep_recent > 0 else []

    if pre_built_summary:
        summary_text = pre_built_summary
        logger.info(
            "[COMPACTION] Using pre-built summary (%d chars) for %d messages",
            len(summary_text),
            len(messages_to_summarize),
        )
    else:
        logger.info(
            "[COMPACTION] Generating sync summary for %d messages",
            len(messages_to_summarize),
        )
        summary_text = await summarize_messages(settings, messages_to_summarize)

    summary_msg = ChatMessage(
        role="user",
        text=(
            "[CONTEXTO DE SESION - Resumen de conversacion anterior]\n\n"
            f"{summary_text}\n\n"
            "[FIN DEL CONTEXTO - La conversacion continua abajo]"
        ),
    )
    new_messages = [summary_msg] + list(recent_messages)
    thread.message_store = ChatMessageStore(messages=new_messages)

    logger.info(
        "[COMPACTION] Done: %d messages -> %d messages (summary + %d recent)",
        len(messages),
        len(new_messages),
        len(recent_messages),
    )
    return True


def should_compact(settings: Settings, message_count: int) -> str | None:
    """Return "hard", "soft", or None based on message count thresholds."""
    if message_count >= settings.chat_v2_compaction_hard_threshold:
        return "hard"
    if message_count >= settings.chat_v2_compaction_soft_threshold:
        return "soft"
    return None
