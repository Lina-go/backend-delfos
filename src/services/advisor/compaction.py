"""Session memory compaction for Advisor agent threads."""

import logging
from typing import Any

from agent_framework import ChatMessage
from agent_framework._threads import ChatMessageStore

from src.config.settings import Settings
from src.orchestrator.handlers._llm_helper import run_handler_agent

logger = logging.getLogger(__name__)

# Lower than chat_v2 (20/40) because advisor generates more messages per turn
# (up to 10 tool calls per question, each producing tool-call + tool-result messages).
SOFT_THRESHOLD = 15
HARD_THRESHOLD = 30
KEEP_RECENT = 4

COMPACTION_SYSTEM_PROMPT = """\
Eres un asistente especializado en resumir conversaciones de analisis financiero \
sobre el sistema financiero colombiano (Superintendencia Financiera).

Tu tarea es crear un resumen estructurado que preserve toda la informacion \
necesaria para continuar la conversacion sin perder contexto.

Formato del resumen:

<resumen_sesion>
## Consultas realizadas
- [Lista de preguntas del usuario con sus respuestas resumidas]

## Entidades analizadas
- [Nombres exactos de bancos/entidades con sus IDs si se mencionaron]

## Metricas y hallazgos clave
- [Metricas consultadas, valores importantes, alertas de severidad]

## Contexto activo
- Ultima consulta: [la pregunta mas reciente]
- Ultimo hallazgo: [breve descripcion del ultimo analisis]
- Entidad en foco: [entidad principal de la conversacion]
- Periodo activo: [rango de fechas en uso]
</resumen_sesion>

Reglas:
1. Escribe TODO en espanol
2. Preserva nombres exactos de entidades y sus IDs
3. Preserva periodos, cifras clave y severidades (CRITICO/ALTO/MODERADO/NORMAL)
4. NO inventes informacion que no este en la conversacion
5. Se conciso pero completo — maximo 500 palabras
6. Ignora errores de herramientas o reintentos — solo el resultado final
"""


def _format_messages_for_summary(messages: list[ChatMessage]) -> str:
    """Convert ChatMessage list to a readable transcript for the summarizer."""
    lines: list[str] = []
    for msg in messages:
        role = msg.role.value if hasattr(msg.role, "value") else str(msg.role)
        text = msg.text or ""

        if not text.strip():
            func_names = []
            for c in msg.contents or []:
                name = getattr(c, "function_name", None) or getattr(c, "name", None)
                if name:
                    func_names.append(name)
            if func_names:
                text = f"[Llamada a herramienta: {', '.join(func_names)}]"
            else:
                continue

        # Truncate long tool results (shorter than chat_v2 because advisor results are bigger)
        if role == "tool" and len(text) > 300:
            text = text[:300] + "... [truncado]"

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
        name="AdvisorCompactor",
        instructions=COMPACTION_SYSTEM_PROMPT,
        message=f"Resume la siguiente conversacion de advisor financiero:\n\n{transcript}",
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
    """Compact a thread's message history by replacing old messages with a summary."""
    store = thread.message_store
    if store is None:
        return False

    messages = store.messages
    if len(messages) <= KEEP_RECENT + 1:
        return False

    cut = len(messages) - KEEP_RECENT if KEEP_RECENT > 0 else len(messages)

    # Don't split a tool_use / tool_result pair.
    while cut > 0 and cut < len(messages):
        role = messages[cut].role
        role_str = role.value if hasattr(role, "value") else str(role)
        if role_str == "tool":
            cut -= 1
        else:
            break

    messages_to_summarize = messages[:cut]
    recent_messages = messages[cut:]

    if pre_built_summary:
        summary_text = pre_built_summary
        logger.info(
            "[ADVISOR COMPACT] Using pre-built summary (%d chars) for %d messages",
            len(summary_text),
            len(messages_to_summarize),
        )
    else:
        logger.info(
            "[ADVISOR COMPACT] Generating sync summary for %d messages",
            len(messages_to_summarize),
        )
        summary_text = await summarize_messages(settings, messages_to_summarize)

    summary_msg = ChatMessage(
        role="user",
        text=(
            "[CONTEXTO DE SESION ADVISOR - Resumen de conversacion anterior]\n\n"
            f"{summary_text}\n\n"
            "[FIN DEL CONTEXTO - La conversacion continua abajo]"
        ),
    )
    new_messages = [summary_msg] + list(recent_messages)
    thread.message_store = ChatMessageStore(messages=new_messages)

    logger.info(
        "[ADVISOR COMPACT] Done: %d messages -> %d (summary + %d recent)",
        len(messages),
        len(new_messages),
        len(recent_messages),
    )
    return True


def should_compact(message_count: int) -> str | None:
    """Return 'hard', 'soft', or None based on message count thresholds."""
    if message_count >= HARD_THRESHOLD:
        return "hard"
    if message_count >= SOFT_THRESHOLD:
        return "soft"
    return None
