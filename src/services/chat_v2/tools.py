"""Chat V2 tools for the single-agent chat."""

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
from src.orchestrator.handlers._llm_helper import run_formatted_handler_agent, run_handler_agent
from src.services.chat_v2.models import UnifiedClassification
from src.services.chat_v2.indicators import compute_full_series_stats, resolve_indicators
from src.services.viz.models import VizColumnMapping
from src.services.viz.service import VisualizationService

logger = logging.getLogger(__name__)

# ContextVar to capture the last execute_and_visualize result per async context.
# The agent.py chat_stream resets this before each request and reads it after streaming.
viz_result_ctx: ContextVar[dict[str, Any] | None] = ContextVar("viz_result", default=None)

# Simple TTL cache for repeated queries (schema lookups, distinct values, etc.)
_tool_cache: dict[str, tuple[float, str]] = {}
_CACHE_TTL = 300  # 5 minutes

# TTL cache for unified LLM classifications — keyed by sorted columns.
# Same column structure almost always produces the same classification.
_classification_cache: dict[str, tuple[float, UnifiedClassification]] = {}
_CLASSIFICATION_CACHE_TTL = 600  # 10 minutes

# Sub-types that support KPI indicators (double filter: Python + LLM prompt)
_INDICATOR_SUBTYPES = {"tendencia_simple", "tendencia_comparada"}


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


def _try_fix_filter_values(sql: str, delfos_tools: DelfosTools) -> str | None:
    """Auto-correct filter values in SQL by checking actual DB values.

    Detects all `column = 'value'` and `column IN ('v1', 'v2')` patterns,
    fetches distinct values for each column, and corrects case/spelling mismatches.
    """
    # Find the main table
    table_match = re.search(r"gold\.(\w+)", sql)
    if not table_match:
        return None
    table = table_match.group(1)

    # Extract column = 'value' patterns from WHERE clauses
    # Matches: COLUMN = 'value', COLUMN IN ('v1', 'v2'), COLUMN = N'value'
    filter_cols: set[str] = set()
    for match in re.finditer(r"(\w+)\s*(?:=|IN\s*\()\s*N?'", sql, re.IGNORECASE):
        col = match.group(1).upper()
        # Skip SQL keywords and numeric/temporal columns
        if col in {"SELECT", "FROM", "WHERE", "AND", "OR", "ON", "AS", "JOIN",
                    "YEAR", "MONTH", "DAY", "NULL", "NOT", "CAST", "ROUND"}:
            continue
        filter_cols.add(match.group(1))  # preserve original case

    if not filter_cols:
        return None

    # Build lookup for each filtered column
    all_lookups: dict[str, dict[str, str]] = {}  # col -> {lower: real}
    for col in filter_cols:
        cache_key = f"distinct:{table}:{col}"
        cached = _cache_get(cache_key)
        if cached:
            raw = cached
        else:
            try:
                raw = delfos_tools.get_distinct_values(table, col)
                _cache_set(cache_key, raw)
            except Exception:
                continue
        real_values = [v.strip() for v in raw.split("\n") if v.strip()]
        all_lookups[col] = {v.lower(): v for v in real_values}

    if not all_lookups:
        return None

    # Fix quoted strings in the SQL
    quoted_strings = re.findall(r"'([^']*)'", sql)
    fixed_sql = sql
    for qs in quoted_strings:
        core = qs.strip("%")
        if not core:
            continue
        # Try each column's lookup
        for _col, lookup in all_lookups.items():
            # Exact case-insensitive match
            real = lookup.get(core.lower())
            if real:
                if "%" in qs:
                    fixed_sql = fixed_sql.replace(f"'{qs}'", f"'%{real}%'")
                else:
                    fixed_sql = fixed_sql.replace(f"'{qs}'", f"'{real}'")
                break
            # Substring match
            for real_val in lookup.values():
                if core.lower() in real_val.lower():
                    if "%" in qs:
                        fixed_sql = fixed_sql.replace(f"'{qs}'", f"'%{real_val}%'")
                    else:
                        fixed_sql = fixed_sql.replace(f"'{qs}'", f"'{real_val}'")
                    break
            else:
                continue
            break

    return fixed_sql if fixed_sql != sql else None


def create_chat_v2_tools(
    delfos_tools: DelfosTools, settings: Settings,
) -> tuple[list[Any], dict[str, Any]]:
    """Create @ai_function tools and result_holder for the Chat V2 agent."""
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
        # Idempotent: ignore duplicate calls within the same turn
        if result_holder.get("clarification"):
            return json.dumps({"already_requested": True}, ensure_ascii=False)

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

        # --- Step 2: Execute SQL (with LLM self-correction on error) ---
        logger.info("[SQL] question=%s | sql=%s", question, sql_query)
        t0 = time.time()
        sql_error: str | None = None
        try:
            result = delfos_tools.execute_sql(sql_query)
            if result.get("error"):
                sql_error = result["error"]
        except Exception as e:
            sql_error = str(e)

        # Self-heal: ask the LLM to fix the SQL if it failed
        if sql_error:
            logger.warning("[SELF-HEAL] SQL failed: %s — asking LLM to fix", sql_error[:120])
            try:
                fixed_sql = await run_handler_agent(
                    settings,
                    name="SQLFixer",
                    instructions=(
                        "Eres un experto en T-SQL para Microsoft Fabric / SQL Server. "
                        "El usuario te da un SQL que falló y el error. "
                        "Devuelve SOLO el SQL corregido, sin explicación ni markdown.\n\n"
                        "Restricciones de T-SQL que DEBES respetar:\n"
                        "- NO uses aggregates anidados: SUM(COUNT(...)) es ilegal.\n"
                        "- NO uses subqueries dentro de funciones de agregación: "
                        "SUM(CASE WHEN x IN (SELECT ...) ...) es ilegal (error 130). "
                        "Solución: materializa el subquery en un CTE o JOIN previo, "
                        "y referencia la columna resultante en el aggregate.\n"
                        "- NO uses LIMIT, usa TOP N.\n"
                        "- NO uses ILIKE, usa LIKE con COLLATE o LOWER().\n"
                        "- NO uses boolean expressions en SELECT; usa CASE WHEN.\n"
                        "- Un CROSS JOIN a un CTE escalar NO resuelve el error 130 "
                        "si el aggregate aún contiene IN (SELECT ...)."
                    ),
                    message=f"SQL:\n{sql_query}\n\nError:\n{sql_error}",
                    model=settings.chat_v2_classifier_model,
                    tools=[],
                    max_tokens=2048,
                    max_iterations=1,
                    temperature=0.0,
                )
                fixed_sql = fixed_sql.strip().strip("`").strip()
                if fixed_sql.lower().startswith("sql"):
                    fixed_sql = fixed_sql[3:].strip()
                is_safe_fix, _ = is_sql_safe(fixed_sql)
                if is_safe_fix and fixed_sql != sql_query:
                    logger.info("[SELF-HEAL] Retrying with LLM-corrected SQL")
                    result = delfos_tools.execute_sql(fixed_sql)
                    if not result.get("error"):
                        sql_query = fixed_sql
                        sql_error = None
            except Exception as fix_exc:
                logger.warning("[SELF-HEAL] LLM fix failed: %s", fix_exc)

        if sql_error:
            return json.dumps(
                {
                    "error": f"Error SQL: {sql_error}. Verifica schema gold y sintaxis T-SQL.",
                    "visualizacion": "NO",
                },
                ensure_ascii=False,
            )

        rows = result.get("data", [])
        t_sql = time.time() - t0
        logger.info("[TIMING] SQL execution: %.2fs (%d rows)", t_sql, len(rows))

        if not rows:
            # Self-healing: try fixing filter values (entity names, segments, etc.)
            corrected = _try_fix_filter_values(sql_query, delfos_tools)
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

        # --- Step 3: Unified classification + column mapping (cached or 1 LLM call) ---
        columns = list(rows[0].keys())
        classification_key = question + "||" + "|".join(sorted(columns))

        # Check classification cache — same columns almost always map the same way
        combined: UnifiedClassification | None = None
        cached_entry = _classification_cache.get(classification_key)
        if cached_entry is not None:
            ts, cached_classification = cached_entry
            if time.time() - ts <= _CLASSIFICATION_CACHE_TTL:
                combined = cached_classification
                logger.info("[TIMING] Classification CACHE HIT for columns: %s", classification_key[:80])
                t_llm = 0.0

        if combined is None:
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

            # Store in cache for future queries with same column structure
            if isinstance(combined, UnifiedClassification):
                _classification_cache[classification_key] = (time.time(), combined)
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

        # Guard: stacked-bar normalizes to 100% — y-axis shows percentages, not raw values
        if chart_type_str == "stackedbar":
            mapping = mapping.model_copy(update={"y_axis_name": "Participación (%)"})

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

        # --- Step 6b: Compute indicators (only for applicable sub_types) ---
        indicator_results: list[dict[str, Any]] = []
        if sub_type_str in _INDICATOR_SUBTYPES and combined.indicators:
            full_stats = compute_full_series_stats(data_points)
            indicator_results = resolve_indicators(full_stats, combined.indicators)
            indicator_results = indicator_results[:3]
            if indicator_results:
                logger.info("[INDICATORS] %d indicators resolved", len(indicator_results))

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
            "indicators": indicator_results,
            "indicator_specs": [s.model_dump() for s in combined.indicators] if combined.indicators else [],
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
            "indicators_summary": [
                {"label": i["label"], "formatted": i["formatted"]} for i in indicator_results
            ],
        }
        return json.dumps(llm_summary, ensure_ascii=False, default=str)

    # ------------------------------------------------------------------
    # modify_chart — tweak active visualization without re-executing SQL
    # ------------------------------------------------------------------
    @ai_function
    def modify_chart(modifications_json: str) -> str:
        """Modifica la gráfica activa sin re-ejecutar SQL.

        Úsala cuando el usuario pida cambiar título, ejes, tipo de gráfico
        u otras propiedades visuales de la gráfica actual.

        Args:
            modifications_json: JSON con los campos a modificar. Campos válidos:
                - titulo_grafica: nuevo título
                - tipo_grafica: "bar" | "line" | "stackedbar" | "pie" | "scatter"
                - y_axis_name: nuevo label del eje Y
                - x_axis_name: nuevo label del eje X
                - metric_name: nuevo nombre de la métrica
                - series_name: nuevo nombre de la serie
                - category_name: nuevo nombre de la categoría

        Returns:
            JSON confirmando los cambios aplicados.
        """
        current_viz = result_holder.get("viz")
        if not current_viz or not current_viz.get("visualization"):
            return json.dumps(
                {"error": "No hay gráfica activa para modificar."},
                ensure_ascii=False,
            )

        try:
            mods = json.loads(modifications_json)
        except json.JSONDecodeError:
            return json.dumps(
                {"error": "JSON inválido en modifications_json."},
                ensure_ascii=False,
            )

        ALLOWED_FIELDS = {
            "titulo_grafica", "tipo_grafica", "y_axis_name", "x_axis_name",
            "metric_name", "series_name", "category_name",
        }

        applied = {}
        for key, value in mods.items():
            if key in ALLOWED_FIELDS:
                current_viz[key] = value
                applied[key] = value

        if not applied:
            return json.dumps(
                {"error": "Ningún campo válido para modificar."},
                ensure_ascii=False,
            )

        result_holder["viz"] = current_viz
        viz_result_ctx.set(current_viz)
        logger.info("[MODIFY_CHART] Applied: %s", applied)

        return json.dumps({"modified": True, "applied": applied}, ensure_ascii=False)

    # ------------------------------------------------------------------
    # Return tools + result holder
    # ------------------------------------------------------------------
    return [request_clarification, execute_and_visualize, modify_chart], result_holder
