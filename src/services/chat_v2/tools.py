"""Chat V2 tools — @ai_function tools for the single-agent chat.

The key tool is `execute_and_visualize` which is a mini-workflow:
  SQL execution → unified classification + column mapping (1 LLM call) → data_points → Power BI.

This guarantees every data response includes visualization + Power BI link,
following the same rules as the V1 pipeline (subtypes.py, guards, build_data_points).
"""

import json
import logging
import re
import time
from contextvars import ContextVar
from typing import Any

from agent_framework import ai_function

from src.config.constants import ChartType
from src.config.prompts.unified_intent_viz import build_unified_intent_viz_prompt
from src.config.settings import Settings
from src.config.subtypes import get_chart_type_for_subtype, get_subtype_from_string
from src.config.validation import is_sql_safe
from src.infrastructure.database.tools import DelfosTools
from src.orchestrator.handlers._llm_helper import run_formatted_handler_agent
from src.services.chat_v2.models import UnifiedClassification
from src.services.viz.models import VizColumnMapping
from src.services.viz.service import VisualizationService

logger = logging.getLogger(__name__)

# ContextVar to capture the last execute_and_visualize result per async context.
# The agent.py chat_stream resets this before each request and reads it after streaming.
viz_result_ctx: ContextVar[dict[str, Any] | None] = ContextVar("viz_result", default=None)

# Simple TTL cache for repeated queries (schema lookups, distinct values, etc.)
_tool_cache: dict[str, tuple[float, str]] = {}
_CACHE_TTL = 300  # 5 minutes


def _cache_get(key: str) -> str | None:
    entry = _tool_cache.get(key)
    if entry is None:
        return None
    ts, value = entry
    if time.time() - ts > _CACHE_TTL:
        del _tool_cache[key]
        return None
    return value


def _cache_set(key: str, value: str) -> str:
    _tool_cache[key] = (time.time(), value)
    return value


def _build_column_stats(
    rows: list[dict[str, Any]], max_unique_shown: int = 15,
) -> dict[str, Any]:
    """Build column cardinality stats for the unified LLM call."""
    if not rows:
        return {}
    columns = list(rows[0].keys())
    stats: dict[str, Any] = {}
    for col in columns:
        unique_vals = list({row.get(col) for row in rows})
        count = len(unique_vals)
        stats[col] = {
            "unique_count": count,
            "unique_values": unique_vals[:max_unique_shown] if count <= max_unique_shown else None,
            "sample_values": unique_vals[:5] if count > max_unique_shown else None,
        }
    return stats


def _try_fix_entity_names(sql: str, delfos_tools: DelfosTools) -> str | None:
    """Auto-correct case-sensitive entity names in SQL.

    When SQL returns 0 rows, this checks if NOMBRE_ENTIDAD filters use wrong casing
    by looking up real values from the warehouse and replacing case-insensitively.
    """
    if "NOMBRE_ENTIDAD" not in sql:
        return None

    table_match = re.search(r"gold\.(\w+)", sql)
    if not table_match:
        return None
    table = table_match.group(1)

    cache_key = f"distinct:{table}:NOMBRE_ENTIDAD"
    cached = _cache_get(cache_key)
    if cached:
        real_names_raw = cached
    else:
        try:
            real_names_raw = delfos_tools.get_distinct_values(table, "NOMBRE_ENTIDAD")
            _cache_set(cache_key, real_names_raw)
        except Exception:
            return None

    real_names = [n.strip() for n in real_names_raw.split("\n") if n.strip()]
    name_lookup = {n.lower(): n for n in real_names}

    quoted_strings = re.findall(r"'([^']*)'", sql)
    fixed_sql = sql
    for qs in quoted_strings:
        core = qs.strip("%")
        if not core:
            continue
        # Exact case-insensitive match
        real = name_lookup.get(core.lower())
        if real:
            if "%" in qs:
                fixed_sql = fixed_sql.replace(f"'{qs}'", f"'%{real}%'")
            else:
                fixed_sql = fixed_sql.replace(f"'{qs}'", f"'{real}'")
            continue
        # Substring match (e.g., 'BOGOTA' → 'Banco de Bogota S.A.')
        for real_name in real_names:
            if core.lower() in real_name.lower():
                if "%" in qs:
                    fixed_sql = fixed_sql.replace(f"'{qs}'", f"'%{real_name}%'")
                else:
                    fixed_sql = fixed_sql.replace(f"'{qs}'", f"'{real_name}'")
                break

    return fixed_sql if fixed_sql != sql else None


def create_chat_v2_tools(
    delfos_tools: DelfosTools, settings: Settings,
) -> tuple[list[Any], dict[str, Any]]:
    """Create @ai_function tools + result_holder for the Chat V2 agent.

    Returns:
        (tools_list, result_holder) — result_holder is a mutable dict with keys
        ``"viz"`` and ``"clarification"`` set by the tools during execution.
        Using a mutable dict instead of ContextVar guarantees propagation
        across any async boundary.
    """
    # VisualizationService for guards + build_data_points + DB insert (LLM call skipped)
    viz_service = VisualizationService(settings, db_tools=delfos_tools)
    result_holder: dict[str, Any] = {"viz": None, "clarification": None}

    # ------------------------------------------------------------------
    # Exploration tools (code-only, fast)
    # ------------------------------------------------------------------

    @ai_function
    def list_tables() -> str:
        """Lista todas las tablas disponibles en el warehouse."""
        cached = _cache_get("list_tables")
        if cached:
            return cached
        try:
            result = delfos_tools.list_tables()
            return _cache_set("list_tables", result)
        except Exception as e:
            return f"Error: {e}"

    @ai_function
    def get_table_schema(table_names: str) -> str:
        """Retorna el esquema (columnas y tipos) de una o más tablas.
        Acepta nombres separados por coma (ej. "distribucion_cartera, tasas_interes_credito").
        Usa esto ANTES de escribir SQL para conocer las columnas exactas."""
        tables = [t.strip().split(".")[-1].strip("[]") for t in table_names.split(",")]
        results = []
        for table in tables:
            if not table:
                continue
            cache_key = f"schema:{table}"
            cached = _cache_get(cache_key)
            if cached:
                results.append(cached)
                continue
            try:
                schema = delfos_tools.get_table_schema(table)
                result_str = f"## {table}\n{schema}"
                _cache_set(cache_key, result_str)
                results.append(result_str)
            except Exception as e:
                results.append(f"## {table}\nError: {e}")
        return "\n\n".join(results)

    @ai_function
    def get_distinct_values(table_name: str, column_name: str) -> str:
        """Retorna los valores únicos de una columna en una tabla.
        Útil para conocer nombres exactos de entidades, segmentos, etc.
        Ejemplo: get_distinct_values("distribucion_cartera", "NOMBRE_ENTIDAD")"""
        bare_table = table_name.strip().split(".")[-1].strip("[]")
        cache_key = f"distinct:{bare_table}:{column_name}"
        cached = _cache_get(cache_key)
        if cached:
            return cached
        try:
            result = delfos_tools.get_distinct_values(bare_table, column_name)
            return _cache_set(cache_key, result)
        except Exception as e:
            return f"Error: {e}"

    # ------------------------------------------------------------------
    # CLARIFICATION TOOL: request_clarification
    # ------------------------------------------------------------------

    @ai_function
    def request_clarification(questions_json: str) -> str:
        """Pide clarificación al usuario cuando hay ambigüedades en la pregunta.

        Usa esta herramienta cuando necesites clarificar UNO O MÁS aspectos antes
        de escribir el SQL: temporalidad, entidad, granularidad, métrica, alcance, etc.
        NO la uses si el usuario está respondiendo a una clarificación previa.

        Args:
            questions_json: JSON array de objetos de clarificación. Cada objeto tiene:
                - "id": identificador corto (ej. "temporalidad", "entidad", "granularidad")
                - "question": la pregunta de clarificación para el usuario
                - "options": array de opciones (strings)

            Ejemplo UNA pregunta:
              [{"id": "temporalidad", "question": "¿Para qué periodo?", "options": ["Últimos 12 meses", "2024", "Todo el histórico", "Otro periodo"]}]

            Ejemplo MÚLTIPLES preguntas:
              [{"id": "temporalidad", "question": "¿Para qué periodo?", "options": ["Últimos 12 meses", "2024", "Histórico"]}, {"id": "alcance", "question": "¿Individual o consolidado?", "options": ["Por banco individual", "Solo consolidado", "Ambos"]}]

        Returns:
            JSON con las preguntas para el frontend.
        """
        try:
            items = json.loads(questions_json)
        except (json.JSONDecodeError, TypeError):
            # Fallback: tratar como pregunta única de texto
            items = [{"id": "general", "question": questions_json, "options": []}]

        if not isinstance(items, list):
            items = [items]

        questions = []
        for item in items:
            q = {
                "id": item.get("id", f"q{len(questions)}"),
                "question": item.get("question", ""),
                "options": item.get("options", []),
            }
            # Si options viene como string pipe-separated (compatibilidad)
            if isinstance(q["options"], str):
                q["options"] = [o.strip() for o in q["options"].split("|") if o.strip()]
            # Ensure "Otro" is always present as last option
            if q["options"] and not any(o.strip().lower().startswith("otro") for o in q["options"]):
                q["options"].append("Otro")
            questions.append(q)

        result = {
            "clarification": True,
            "questions": questions,
        }
        result_holder["clarification"] = result
        logger.info("[CLARIFICATION] %d questions: %s", len(questions), [q["id"] for q in questions])
        return json.dumps(result, ensure_ascii=False)

    # ------------------------------------------------------------------
    # THE WORKFLOW TOOL: execute_and_visualize
    # ------------------------------------------------------------------

    @ai_function
    async def execute_and_visualize(question: str, sql_query: str) -> str:
        """Ejecuta SQL y genera automáticamente visualización + link Power BI.

        SIEMPRE usa esta herramienta para responder preguntas con datos.
        NO existe otra forma de ejecutar SQL — esta herramienta se encarga de todo:
        1. Valida y ejecuta el SQL
        2. Clasifica el tipo de análisis Y mapea columnas (1 sola llamada LLM)
        3. Determina el tipo de gráfico apropiado
        4. Genera los data_points para la gráfica
        5. Genera el link de Power BI

        Args:
            question: La pregunta original del usuario (necesaria para clasificar el intent)
            sql_query: Consulta SQL SELECT a ejecutar

        Returns:
            JSON con datos, tipo de gráfica, data_points y link de Power BI.
        """
        t_total = time.time()

        # --- Step 1: Validate SQL ---
        is_safe, reason = is_sql_safe(sql_query)
        if not is_safe:
            return json.dumps(
                {"error": f"SQL rechazada por seguridad: {reason}", "visualizacion": "NO"},
                ensure_ascii=False,
            )

        # --- Step 2: Execute SQL ---
        logger.info("[SQL] question=%s | sql=%s", question, sql_query)
        t0 = time.time()
        try:
            result = delfos_tools.execute_sql(sql_query)
            if result.get("error"):
                return json.dumps(
                    {
                        "error": f"Error SQL: {result['error']}. Verifica schema gold y sintaxis T-SQL.",
                        "visualizacion": "NO",
                    },
                    ensure_ascii=False,
                )
            rows = result.get("data", [])
        except Exception as e:
            return json.dumps(
                {"error": f"Error SQL: {e}. Verifica schema gold y sintaxis T-SQL.", "visualizacion": "NO"},
                ensure_ascii=False,
            )
        t_sql = time.time() - t0
        logger.info("[TIMING] SQL execution: %.2fs (%d rows)", t_sql, len(rows))

        if not rows:
            # Self-healing: try fixing case-sensitive entity names
            corrected = _try_fix_entity_names(sql_query, delfos_tools)
            if corrected:
                logger.info("[SELF-HEAL] Retrying with corrected SQL: %s", corrected)
                t0_retry = time.time()
                try:
                    result = delfos_tools.execute_sql(corrected)
                    rows = result.get("data", [])
                except Exception:
                    rows = []
                t_retry = time.time() - t0_retry
                logger.info("[SELF-HEAL] Retry: %.2fs (%d rows)", t_retry, len(rows))
                if rows:
                    sql_query = corrected

        if not rows:
            return json.dumps(
                {"error": "Sin resultados.", "visualizacion": "NO"},
                ensure_ascii=False,
            )

        # --- Step 3: Unified classification + column mapping (1 LLM call) ---
        columns = list(rows[0].keys())
        n = len(rows)
        if n <= 5:
            sample_rows = rows
        else:
            step = max(1, n // 5)
            sample_rows = [rows[i * step] for i in range(min(5, n))]
        column_stats = _build_column_stats(rows)

        t0 = time.time()
        try:
            combined = await run_formatted_handler_agent(
                settings,
                name="UnifiedClassifier",
                instructions=build_unified_intent_viz_prompt(),
                message=json.dumps(
                    {
                        "question": question,
                        "columns": columns,
                        "sample_rows": sample_rows,
                        "column_stats": column_stats,
                    },
                    ensure_ascii=False,
                    default=str,
                ),
                response_format=UnifiedClassification,
                model=settings.chat_v2_classifier_model,
                tools=[],
                max_tokens=1024,
                temperature=0.0,
            )
        except Exception as e:
            logger.error("Unified classification failed: %s", e, exc_info=True)
            return json.dumps(
                {
                    "error": f"Error en clasificación: {e}",
                    "visualizacion": "NO",
                    "datos": rows[:10],
                    "sql_query": sql_query,
                },
                ensure_ascii=False,
                default=str,
            )
        t_llm = time.time() - t0
        logger.info("[TIMING] Unified LLM classification: %.2fs", t_llm)

        if not isinstance(combined, UnifiedClassification):
            logger.error("Unified classifier did not return expected model")
            return json.dumps(
                {
                    "error": "Error en clasificación unificada",
                    "visualizacion": "NO",
                    "datos": rows[:10],
                    "sql_query": sql_query,
                },
                ensure_ascii=False,
                default=str,
            )

        # --- Step 4: Determine chart type (deterministic from sub_type) ---
        sub_type_str = combined.sub_type
        sub_type_enum = get_subtype_from_string(sub_type_str)
        logger.info(
            "[TIMING] Classification result: sub_type=%s, titulo=%s, is_tasa=%s",
            sub_type_str, combined.titulo_grafica, combined.is_tasa,
        )

        chart_type_enum: ChartType | None = None
        if sub_type_enum is not None:
            try:
                chart_type_enum = get_chart_type_for_subtype(sub_type_enum)
            except ValueError:
                chart_type_enum = ChartType.BAR

        # valor_puntual → no chart, just data
        if chart_type_enum is None:
            logger.info("[TIMING] valor_puntual — no chart. Total: %.2fs", time.time() - t_total)
            data_preview = rows[:20] if len(rows) > 20 else rows
            return json.dumps(
                {
                    "visualizacion": "NO",
                    "datos": data_preview,
                    "total_filas": len(rows),
                    "sql_query": sql_query,
                },
                ensure_ascii=False,
                default=str,
            )

        chart_type_str = chart_type_enum.value
        logger.info("[TIMING] Chart type: %s (from sub_type=%s)", chart_type_str, sub_type_str)

        # --- Step 5: Extract VizColumnMapping from combined result ---
        if not combined.x_column or not combined.y_column:
            logger.warning(
                "[TIMING] Missing x_column/y_column for sub_type=%s — treating as valor_puntual",
                sub_type_str,
            )
            data_preview = rows[:20] if len(rows) > 20 else rows
            return json.dumps(
                {
                    "visualizacion": "NO",
                    "datos": data_preview,
                    "total_filas": len(rows),
                    "sql_query": sql_query,
                },
                ensure_ascii=False,
                default=str,
            )

        mapping = VizColumnMapping(
            x_column=combined.x_column,
            y_column=combined.y_column,
            month_column=combined.month_column,
            series_column=combined.series_column,
            category_column=combined.category_column,
            x_format=combined.x_format,
            metric_name=combined.metric_name or "Valor",
            x_axis_name=combined.x_axis_name or "",
            y_axis_name=combined.y_axis_name or "",
            series_name=combined.series_name,
            category_name=combined.category_name,
        )
        logger.info(
            "[TIMING] Mapping: x=%s, y=%s, month=%s, series=%s, category=%s",
            mapping.x_column, mapping.y_column, mapping.month_column,
            mapping.series_column, mapping.category_column,
        )

        # --- Step 6: Guards + build_data_points + DB insert (via VisualizationService) ---
        t0 = time.time()
        try:
            viz_result = await viz_service.generate(
                sql_results=rows,
                user_id="chat_v2",
                question=question,
                sql_query=sql_query,
                chart_type=chart_type_str,
                sub_type=sub_type_str,
                precomputed_mapping=mapping,
            )
        except Exception as e:
            logger.error("VisualizationService.generate failed: %s", e, exc_info=True)
            return json.dumps(
                {
                    "error": f"Error generando visualización: {e}",
                    "visualizacion": "NO",
                    "datos": rows[:10],
                    "sql_query": sql_query,
                },
                ensure_ascii=False,
                default=str,
            )
        t_viz = time.time() - t0
        logger.info("[TIMING] VizService.generate (guards+data_points+DB): %.2fs", t_viz)

        data_points = viz_result.get("data_points", [])
        logger.info("[TIMING] data_points count: %d", len(data_points))
        if not data_points:
            logger.warning("[TIMING] No data_points generated! viz_result keys: %s", list(viz_result.keys()))
            return json.dumps(
                {
                    "visualizacion": "NO",
                    "error": "No se pudieron generar data_points",
                    "datos": rows[:20],
                    "sql_query": sql_query,
                },
                ensure_ascii=False,
                default=str,
            )

        # --- Step 7: Build response (same format as V1 ResponseFormatter) ---
        titulo = combined.titulo_grafica or question[:60]
        is_tasa = combined.is_tasa

        response = {
            "visualization": True,
            "visualizacion": "YES",
            "patron": "chat_v2",
            "tipo_grafica": chart_type_str,
            "titulo_grafica": titulo,
            "data_points": data_points,
            "metric_name": viz_result.get("metric_name", "Valor"),
            "x_axis_name": viz_result.get("x_axis_name", ""),
            "y_axis_name": viz_result.get("y_axis_name", ""),
            "series_name": viz_result.get("series_name"),
            "category_name": viz_result.get("category_name"),
            "is_tasa": is_tasa,
            "link_power_bi": viz_result.get("powerbi_url", ""),
            "sql_query": sql_query,
            "total_filas": len(rows),
            "run_id": viz_result.get("run_id", ""),
            "datos": rows[:50],
            "original_question": question,
        }

        t_total_elapsed = time.time() - t_total
        logger.info(
            "[TIMING] execute_and_visualize TOTAL: %.2fs (SQL=%.2fs, LLM=%.2fs, Viz=%.2fs) | chart=%s, points=%d",
            t_total_elapsed, t_sql, t_llm, t_viz, chart_type_str, len(data_points),
        )

        # Store FULL result for chat_stream to emit as guaranteed SSE event
        result_holder["viz"] = response
        viz_result_ctx.set(response)  # Keep ContextVar as fallback

        # Return SHORT summary to the LLM (for the text insight).
        # The full data (data_points, datos) goes to the frontend via the
        # sentinel mechanism — no need to bloat the LLM context with 500+ rows
        # which triggers 429 rate-limits on the follow-up text-generation call.
        llm_summary = {
            "visualization": True,
            "tipo_grafica": chart_type_str,
            "titulo_grafica": titulo,
            "total_filas": len(rows),
            "data_points_count": len(data_points),
            "metric_name": viz_result.get("metric_name", "Valor"),
            "is_tasa": is_tasa,
            "sample_data": rows[:5],
        }
        return json.dumps(llm_summary, ensure_ascii=False, default=str)

    # ------------------------------------------------------------------
    # Return tools + result holder
    # ------------------------------------------------------------------
    # request_clarification + execute_and_visualize exposed to the agent.
    # Exploration tools remain defined for internal use by _try_fix_entity_names.
    return [request_clarification, execute_and_visualize], result_holder
