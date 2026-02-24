"""Advisor agent tools that wrap DelfosTools for warehouse access."""

import re
import statistics
import time
from typing import Any

from agent_framework import ai_function

from src.config.validation import is_sql_safe
from src.infrastructure.database.tools import DelfosTools
from src.services.analysis.correlation import compute_relationship_stats

# Simple TTL cache for frequently repeated queries (lookup_entity, latest_date, etc.)
_tool_cache: dict[str, tuple[float, str]] = {}
_CACHE_TTL = 300  # 5 minutes


def _cache_get(key: str) -> str | None:
    """Return cached value if still valid, else None."""
    entry = _tool_cache.get(key)
    if entry is None:
        return None
    ts, value = entry
    if time.time() - ts > _CACHE_TTL:
        del _tool_cache[key]
        return None
    return value


def _cache_set(key: str, value: str) -> str:
    """Store value in cache and return it."""
    _tool_cache[key] = (time.time(), value)
    return value


def _validate_advisor_param(value: str, param_name: str, max_length: int = 20) -> str:
    """Valida un parámetro que se interpolará en SQL.

    Solo permite caracteres alfanuméricos, espacios, guiones, puntos y tildes.
    Rechaza cualquier palabra reservada SQL peligrosa.
    """
    value = value.strip()
    if len(value) > max_length:
        raise ValueError(f"{param_name} excede longitud máxima {max_length}")
    if not re.fullmatch(r'[a-zA-ZáéíóúÁÉÍÓÚñÑ0-9\s\-\.]+', value):
        raise ValueError(f"{param_name} contiene caracteres no permitidos")
    blocked_keywords = ("union", "select", "insert", "update", "delete", "drop",
                        "exec", "execute", "xp_", "sp_", "alter", "create", "truncate")
    lower = value.lower()
    for kw in blocked_keywords:
        if kw in lower.split():
            raise ValueError(f"{param_name} contiene palabra SQL no permitida: {kw!r}")
    return value


def _fecha_filter(fecha_corte: str) -> str:
    """Convert MMYYYY into SQL filter: year = YYYY AND month = MM."""
    fecha_corte = fecha_corte.strip()
    if len(fecha_corte) < 5 or len(fecha_corte) > 8:
        raise ValueError(
            f"fecha_corte invalida: '{fecha_corte}'. Formato esperado: MMYYYY (ej. '102025')"
        )
    month_str = fecha_corte[: len(fecha_corte) - 4]
    year_str = fecha_corte[len(fecha_corte) - 4 :]
    try:
        month, year = int(month_str), int(year_str)
    except ValueError:
        raise ValueError(
            f"fecha_corte invalida: '{fecha_corte}'. Formato esperado: MMYYYY (ej. '102025')"
        )
    if not (1 <= month <= 12):
        raise ValueError(f"Mes invalido: {month}. Debe estar entre 1 y 12.")
    return f"year = {year} AND month = {month}"


def create_advisor_tools(delfos_tools: DelfosTools) -> list[Any]:
    """Create advisor @ai_function tools backed by the shared DelfosTools instance."""

    @ai_function
    def query_warehouse(sql: str) -> str:
        """Ejecuta SQL SELECT contra el warehouse. Solo SELECT permitido. Tablas en schema gold."""
        is_safe, reason = is_sql_safe(sql)
        if not is_safe:
            return f"Error: SQL rechazada: {reason}"
        try:
            result = delfos_tools.execute_sql(sql)
            if result.get("error"):
                return f"Error SQL: {result['error']}"
            rows = result.get("data", [])
            if not rows:
                return "Sin resultados."
            cols = list(rows[0].keys())
            header = " | ".join(cols)
            lines = [header, "-" * len(header)]
            for row in rows[:50]:
                lines.append(" | ".join(str(row.get(c, "")) for c in cols))
            return f"{len(rows)} filas:\n" + "\n".join(lines)
        except Exception as e:
            return f"Error SQL: {e}. Usa schema gold (ej. gold.distribucion_cartera)."

    @ai_function
    def get_available_tables() -> str:
        """Lista todas las tablas disponibles en el warehouse de la SFC."""
        try:
            return delfos_tools.list_tables()
        except Exception as e:
            return f"Error al listar tablas: {e}"

    @ai_function
    def get_table_columns(table_name: str) -> str:
        """Obtiene el schema (columnas y tipos) de una tabla. Acepta "distribucion_cartera" o "gold.distribucion_cartera"."""
        try:
            bare_name = table_name.split(".")[-1].strip("[]")
            return delfos_tools.get_table_schema(bare_name)
        except Exception as e:
            return f"Error al obtener schema de {table_name}: {e}"

    @ai_function
    def trend_analysis(entity_id: str, metric: str, n_periods: int = 12) -> str:
        """Calcula tendencias historicas MoM, QoQ, YoY y CAGR de una entidad. Metric: "cartera" | "tasa_credito" | "tasa_captacion"."""
        try:
            entity_id = _validate_advisor_param(entity_id, "entity_id")
        except ValueError as e:
            return f"Error de validación: {e}"
        n_periods = max(3, min(24, n_periods))
        sql_map = {
            "cartera": (
                f"SELECT TOP {n_periods} year, month, "
                f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS valor "
                f"FROM gold.distribucion_cartera "
                f"WHERE ID_ENTIDAD = '{entity_id}' "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
            "tasa_credito": (
                f"SELECT TOP {n_periods} year, month, "
                f"AVG(TASA_EFECTIVA_PROMEDIO) AS valor "
                f"FROM gold.tasas_interes_credito "
                f"WHERE ID_ENTIDAD = '{entity_id}' "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
            "tasa_captacion": (
                f"SELECT TOP {n_periods} year, month, "
                f"AVG(TASA) AS valor "
                f"FROM gold.tasas_interes_captacion "
                f"WHERE ID_ENTIDAD = '{entity_id}' "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
        }
        if metric not in sql_map:
            return f"metric debe ser uno de: {list(sql_map.keys())}"

        result = delfos_tools.execute_sql(sql_map[metric])
        if result.get("error"):
            return f"Error SQL: {result['error']}"
        rows = result.get("data", [])
        if len(rows) < 2:
            return "Datos insuficientes (se necesitan 2+ periodos)."

        rows = [r for r in reversed(rows) if r.get("valor") is not None]
        if len(rows) < 2:
            return "Datos insuficientes (se necesitan 2+ periodos con valores no nulos)."
        vals = [float(r["valor"]) for r in rows]
        labels = [f"{int(r['year'])}-{int(r['month']):02d}" for r in rows]

        mom = [
            (vals[i] - vals[i - 1]) / abs(vals[i - 1]) * 100 if vals[i - 1] != 0 else None
            for i in range(1, len(vals))
        ]

        out = [f"Tendencia {metric} | Entidad {entity_id} | {len(rows)} periodos", ""]
        out.append(f"{'Periodo':<12} | {'Valor':>14} | MoM%")
        out.append("-" * 40)
        for i, (lbl, val) in enumerate(zip(labels, vals)):
            mom_str = f"{mom[i-1]:+.1f}%" if i > 0 and mom[i - 1] is not None else "  base"
            out.append(f"{lbl:<12} | {val:>14,.0f} | {mom_str}")

        if len(vals) >= 4 and vals[-4] != 0:
            out.append(f"QoQ: {(vals[-1] - vals[-4]) / abs(vals[-4]) * 100:+.1f}%")
        if len(vals) >= 13 and vals[-13] != 0:
            out.append(f"YoY: {(vals[-1] - vals[-13]) / abs(vals[-13]) * 100:+.1f}%")
        if len(vals) >= 12 and vals[0] != 0:
            cagr = (vals[-1] / vals[0]) ** (12.0 / len(vals)) - 1
            out.append(f"CAGR ({len(vals)} meses): {cagr * 100:+.1f}%")

        inflexions = [
            f"{labels[i + 1]} (cambio a {'positivo' if mom[i] > 0 else 'negativo'})"
            for i in range(1, len(mom))
            if mom[i] is not None and mom[i - 1] is not None and (mom[i] > 0) != (mom[i - 1] > 0)
        ]
        if inflexions:
            out.append(f"Inflexiones: {', '.join(inflexions)}")

        return "\n".join(out)

    @ai_function
    def peer_benchmark(entity_id: str, metric: str, fecha_corte: str) -> str:
        """Compara una entidad contra todos sus peers en el mercado. Metric: "cartera" | "tasa_credito" | "tasa_captacion"."""
        try:
            entity_id = _validate_advisor_param(entity_id, "entity_id")
            fecha_corte = _validate_advisor_param(fecha_corte, "fecha_corte", max_length=10)
        except ValueError as e:
            return f"Error de validación: {e}"
        sql_map = {
            "cartera": (
                f"SELECT ID_ENTIDAD, MAX(NOMBRE_ENTIDAD) AS nombre, "
                f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS valor "
                f"FROM gold.distribucion_cartera "
                f"WHERE {_fecha_filter(fecha_corte)} "
                f"GROUP BY ID_ENTIDAD ORDER BY valor DESC"
            ),
            "tasa_credito": (
                f"SELECT ID_ENTIDAD, MAX(NOMBRE_ENTIDAD) AS nombre, "
                f"AVG(TASA_EFECTIVA_PROMEDIO) AS valor "
                f"FROM gold.tasas_interes_credito "
                f"WHERE {_fecha_filter(fecha_corte)} "
                f"GROUP BY ID_ENTIDAD ORDER BY valor DESC"
            ),
            "tasa_captacion": (
                f"SELECT ID_ENTIDAD, MAX(NOMBRE_ENTIDAD) AS nombre, "
                f"AVG(TASA) AS valor "
                f"FROM gold.tasas_interes_captacion "
                f"WHERE {_fecha_filter(fecha_corte)} "
                f"GROUP BY ID_ENTIDAD ORDER BY valor DESC"
            ),
        }
        if metric not in sql_map:
            return f"metric debe ser: {list(sql_map.keys())}"

        result = delfos_tools.execute_sql(sql_map[metric])
        if result.get("error"):
            return f"Error SQL: {result['error']}"
        rows = result.get("data", [])
        if not rows:
            return f"Sin datos para {metric} en {fecha_corte}."

        total = sum(float(r["valor"]) for r in rows if r["valor"] is not None)
        entity_rank, entity_val, entity_name = None, None, str(entity_id)
        for rank, row in enumerate(rows, 1):
            if str(row["ID_ENTIDAD"]) == str(entity_id):
                entity_rank = rank
                entity_val = float(row["valor"])
                entity_name = row.get("nombre") or str(entity_id)
                break

        leader_val = float(rows[0]["valor"]) if rows[0]["valor"] else 0
        avg_val = total / len(rows) if rows else 0

        out = [f"Benchmark {metric} | Periodo {fecha_corte} | {len(rows)} entidades", ""]
        out.append(f"{'#':<4} {'Entidad':<30} {'Valor':>16} {'Share':>8}")
        out.append("-" * 62)
        for rank, row in enumerate(rows[:10], 1):
            val = float(row["valor"]) if row["valor"] is not None else 0
            share = val / total * 100 if total else 0
            marker = " <--" if str(row["ID_ENTIDAD"]) == str(entity_id) else ""
            nombre = row.get("nombre") or str(row["ID_ENTIDAD"])
            out.append(f"{rank:<4} {nombre:<30} {val:>16,.0f} {share:>7.1f}%{marker}")

        out.append("")
        if entity_rank is not None and entity_val is not None:
            entity_share = entity_val / total * 100 if total else 0
            gap_pct = (entity_val / leader_val - 1) * 100 if leader_val else 0
            out.append(f"Entidad {entity_name}: posicion #{entity_rank} | share {entity_share:.1f}%")
            out.append(
                f"Gap vs lider: {leader_val - entity_val:,.0f} ({gap_pct:.1f}%) | "
                f"Promedio mercado: {avg_val:,.0f}"
            )
        else:
            out.append(f"Entidad {entity_id} no encontrada en {fecha_corte}.")

        return "\n".join(out)

    @ai_function
    def detect_anomalies(entity_id: str, lookback_months: int = 12) -> str:
        """Detecta anomalias estadisticas en una entidad: ICV z-score, market share loss, concentracion HHI y outlier de cartera."""
        try:
            entity_id = _validate_advisor_param(entity_id, "entity_id")
        except ValueError as e:
            return f"Error de validación: {e}"
        lookback_months = max(6, min(24, lookback_months))
        alerts: list[str] = []

        # Single query for entity history: ICV + cartera total + vigente (replaces r1, r2, r4)
        r_entity = delfos_tools.execute_sql(
            f"SELECT TOP {lookback_months} year, month, "
            f"SUM(CAST(SALDO_CARTERA_VIGENTE AS BIGINT)) AS vigente, "
            f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS total "
            f"FROM gold.distribucion_cartera "
            f"WHERE ID_ENTIDAD = '{entity_id}' "
            f"GROUP BY year, month ORDER BY year DESC, month DESC"
        )

        entity_data = r_entity.get("data", []) if not r_entity.get("error") else []

        # --- ICV z-score ---
        if entity_data:
            icv_series = [
                float(r["vigente"]) / float(r["total"]) * 100
                for r in entity_data
                if r.get("vigente") is not None and r.get("total") is not None and float(r["total"]) > 0
            ]
            if len(icv_series) >= 3:
                mean_icv = statistics.mean(icv_series[1:])
                std_icv = statistics.stdev(icv_series[1:]) if len(icv_series) > 2 else 0
                if std_icv > 0:
                    z = (icv_series[0] - mean_icv) / std_icv
                    if z < -3:
                        alerts.append(
                            f"CRITICO  | ICV | {icv_series[0]:.2f}% (z={z:.1f}) vs media {mean_icv:.2f}%"
                        )
                    elif z < -2:
                        alerts.append(
                            f"ATENCION | ICV | {icv_series[0]:.2f}% (z={z:.1f}) vs media {mean_icv:.2f}%"
                        )

        # --- Market share loss (only need market totals as second query) ---
        if entity_data and len(entity_data) >= 2:
            r_mkt = delfos_tools.execute_sql(
                f"SELECT TOP {lookback_months} year, month, "
                f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS market_total "
                f"FROM gold.distribucion_cartera "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            )
            if not r_mkt.get("error") and r_mkt.get("data"):
                ent = {
                    (r["year"], r["month"]): float(r["total"])
                    for r in entity_data if r["total"]
                }
                mkt = {
                    (r["year"], r["month"]): float(r["market_total"])
                    for r in r_mkt["data"] if r["market_total"]
                }
                shares = [
                    ent[k] / mkt[k] * 100
                    for k in sorted(ent) if k in mkt and mkt[k] > 0
                ]
                if len(shares) >= 2:
                    mom_share = shares[-1] - shares[-2]
                    if mom_share < -5:
                        alerts.append(
                            f"CRITICO  | Market share | Caida MoM {mom_share:.2f}pp (actual {shares[-1]:.2f}%)"
                        )
                    elif mom_share < -2:
                        neg_streak = 0
                        for i in range(len(shares) - 1, 0, -1):
                            if shares[i] < shares[i - 1]:
                                neg_streak += 1
                            else:
                                break
                        if neg_streak >= 3:
                            alerts.append(
                                f"ATENCION | Market share | Caida MoM {mom_share:.2f}pp, "
                                f"{neg_streak} periodos consecutivos"
                            )

        # --- HHI concentracion ---
        r3 = delfos_tools.execute_sql(
            f"SELECT SEGMENTO, SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS val "
            f"FROM gold.distribucion_cartera "
            f"WHERE ID_ENTIDAD = '{entity_id}' AND SEGMENTO IS NOT NULL "
            f"AND FECHA_CORTE = (SELECT MAX(FECHA_CORTE) FROM gold.distribucion_cartera "
            f"WHERE ID_ENTIDAD = '{entity_id}') "
            f"GROUP BY SEGMENTO"
        )
        if not r3.get("error") and r3.get("data") and len(r3["data"]) > 1:
            segs = [float(r["val"]) for r in r3["data"] if r["val"]]
            total_seg = sum(segs)
            if total_seg > 0:
                hhi = sum((s / total_seg) ** 2 for s in segs)
                if hhi > 0.5:
                    alerts.append(f"CRITICO  | HHI concentracion | {hhi:.3f} (umbral 0.5)")
                elif hhi > 0.25:
                    alerts.append(f"ATENCION | HHI concentracion | {hhi:.3f} (umbral 0.25)")

        # --- Cartera outlier (reuses entity_data from r_entity) ---
        if entity_data and len(entity_data) >= 4:
            vals = [float(r["total"]) for r in entity_data if r["total"]]
            if len(vals) >= 4:
                mean_v = statistics.mean(vals[1:])
                std_v = statistics.stdev(vals[1:]) if len(vals) > 2 else 0
                if std_v > 0:
                    z = (vals[0] - mean_v) / std_v
                    if abs(z) > 3:
                        alerts.append(f"CRITICO  | Cartera outlier | z={z:.1f} vs historia")
                    elif abs(z) > 2:
                        alerts.append(f"ATENCION | Cartera outlier | z={z:.1f} vs historia")

        if not alerts:
            return f"Sin anomalias en los ultimos {lookback_months} meses para entidad {entity_id}."
        return "\n".join([f"Anomalias detectadas (entidad {entity_id}):", ""] + alerts)

    @ai_function
    def pricing_analysis(entity_id: str, tipo_credito: str, fecha_corte: str) -> str:
        """Analiza el pricing de credito de una entidad vs el mercado. tipo_credito es TIPO_DE_CR_DITO (ej. "Consumo", "Comercial")."""
        try:
            entity_id = _validate_advisor_param(entity_id, "entity_id")
            tipo_credito = _validate_advisor_param(tipo_credito, "tipo_credito", max_length=50)
            fecha_corte = _validate_advisor_param(fecha_corte, "fecha_corte", max_length=10)
        except ValueError as e:
            return f"Error de validación: {e}"
        result = delfos_tools.execute_sql(
            f"SELECT ID_ENTIDAD, MAX(NOMBRE_ENTIDAD) AS nombre, "
            f"AVG(TASA_EFECTIVA_PROMEDIO) AS tasa, "
            f"SUM(CAST(MONTOS_DESEMBOLSADOS AS BIGINT)) AS volumen "
            f"FROM gold.tasas_interes_credito "
            f"WHERE TIPO_DE_CR_DITO = '{tipo_credito}' AND {_fecha_filter(fecha_corte)} "
            f"GROUP BY ID_ENTIDAD ORDER BY tasa DESC"
        )
        if result.get("error"):
            return f"Error SQL: {result['error']}"
        rows = result.get("data", [])
        if not rows:
            return f"Sin datos para tipo_credito={tipo_credito} en {fecha_corte}."

        total_vol = sum(float(r["volumen"]) for r in rows if r["volumen"])
        tasa_mercado = (
            sum(float(r["tasa"]) * float(r["volumen"]) for r in rows if r["tasa"] and r["volumen"])
            / total_vol
            if total_vol > 0
            else statistics.mean([float(r["tasa"]) for r in rows if r["tasa"]])
        )

        entity_tasa, entity_name, entity_rank = None, str(entity_id), None
        for rank, row in enumerate(rows, 1):
            if str(row["ID_ENTIDAD"]) == str(entity_id):
                entity_tasa = float(row["tasa"]) if row["tasa"] else None
                entity_name = row.get("nombre") or str(entity_id)
                entity_rank = rank
                break

        out = [f"Pricing {tipo_credito} | Periodo {fecha_corte} | {len(rows)} entidades", ""]
        out.append(f"Tasa mercado (weighted avg): {tasa_mercado:.2f}%")

        if entity_tasa is not None:
            spread_bps = (entity_tasa - tasa_mercado) * 100
            posicion = "PREMIUM" if spread_bps > 10 else ("DISCOUNT" if spread_bps < -10 else "EN LINEA")
            out.append(
                f"Entidad {entity_name}: tasa {entity_tasa:.2f}% | "
                f"spread {spread_bps:+.0f} bps | {posicion}"
            )
            out.append(f"Ranking por tasa: #{entity_rank} de {len(rows)}")
        else:
            out.append(f"Entidad {entity_id} no encontrada en {fecha_corte} para {tipo_credito}.")

        hist = delfos_tools.execute_sql(
            f"SELECT TOP 6 FECHA_CORTE, "
            f"AVG(TASA_EFECTIVA_PROMEDIO) AS tasa, SUM(MONTOS_DESEMBOLSADOS) AS volumen "
            f"FROM gold.tasas_interes_credito "
            f"WHERE ID_ENTIDAD = '{entity_id}' AND TIPO_DE_CR_DITO = '{tipo_credito}' "
            f"GROUP BY FECHA_CORTE ORDER BY FECHA_CORTE DESC"
        )
        if not hist.get("error") and hist.get("data") and len(hist["data"]) >= 6:
            pts = [
                {"x_value": float(r["tasa"]), "y_value": float(r["volumen"])}
                for r in hist["data"]
                if r["tasa"] and r["volumen"]
            ]
            if len(pts) >= 6:
                stats = compute_relationship_stats(pts)
                if "r" in stats:
                    r_val = stats["r"]
                    elasticity = (
                        "inversa (subir tasa reduce volumen)" if r_val < -0.5
                        else ("directa" if r_val > 0.5 else "sin correlacion clara")
                    )
                    out.append(f"Elasticidad tasa-volumen: r={r_val:.2f} ({elasticity})")

        return "\n".join(out)

    @ai_function
    def get_entity_profile(entity_id: str) -> str:
        """Retorna perfil basico de una entidad: nombre, tipo y rango de datos. Util como primer paso para confirmar la entidad."""
        try:
            entity_id = _validate_advisor_param(entity_id, "entity_id")
        except ValueError as e:
            return f"Error de validación: {e}"

        r_banco = delfos_tools.execute_sql(
            f"SELECT NOMBRE_ENTIDAD, TIPO_ENTIDAD, NOMBRE_TIPO_ENTIDAD "
            f"FROM gold.banco WHERE ID_ENTIDAD = '{entity_id}'"
        )
        banco_rows = r_banco.get("data", []) if not r_banco.get("error") else []
        if banco_rows:
            b = banco_rows[0]
            nombre = b.get("NOMBRE_ENTIDAD") or str(entity_id)
            nombre_tipo = b.get("NOMBRE_TIPO_ENTIDAD") or "N/A"
            tipo = b.get("TIPO_ENTIDAD") or "N/A"
        else:
            nombre = f"Entidad {entity_id} (no en gold.banco)"
            nombre_tipo = "Desconocido"
            tipo = "N/A"

        r_range = delfos_tools.execute_sql(
            f"SELECT MIN(FECHA_CORTE) AS fecha_min, MAX(FECHA_CORTE) AS fecha_max, "
            f"COUNT(DISTINCT FECHA_CORTE) AS n_periodos "
            f"FROM gold.distribucion_cartera WHERE ID_ENTIDAD = '{entity_id}'"
        )
        r_last = delfos_tools.execute_sql(
            f"SELECT TOP 1 FECHA_CORTE, "
            f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS saldo_total "
            f"FROM gold.distribucion_cartera WHERE ID_ENTIDAD = '{entity_id}' "
            f"GROUP BY FECHA_CORTE ORDER BY FECHA_CORTE DESC"
        )

        out = [f"Perfil de Entidad | ID: {entity_id}", "",
               f"Nombre:     {nombre}",
               f"Tipo:       {nombre_tipo} (codigo: {tipo})", ""]
        if not r_range.get("error") and r_range.get("data"):
            rr = r_range["data"][0]
            out += [
                "Datos disponibles en cartera:",
                f"  Primer periodo: {rr.get('fecha_min', 'N/A')}",
                f"  Ultimo periodo: {rr.get('fecha_max', 'N/A')}",
                f"  Total periodos: {rr.get('n_periodos', 0)}",
            ]
        if not r_last.get("error") and r_last.get("data"):
            rl = r_last["data"][0]
            saldo = float(rl.get("saldo_total") or 0)
            out.append(
                f"  Saldo cartera ultimo periodo: {saldo:,.0f}  ({rl.get('FECHA_CORTE', 'N/A')})"
            )
        return "\n".join(out)

    @ai_function
    def get_portfolio_breakdown(entity_id: str, fecha_corte: str) -> str:
        """Desglosa cartera por segmento (Comercial/Consumo/Vivienda/Microcredito) con ICV, share de mercado y top-5 categorias."""
        try:
            entity_id = _validate_advisor_param(entity_id, "entity_id")
            fecha_corte = _validate_advisor_param(fecha_corte, "fecha_corte", max_length=10)
        except ValueError as e:
            return f"Error de validación: {e}"

        r_ent = delfos_tools.execute_sql(
            f"SELECT SEGMENTO, "
            f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS total, "
            f"SUM(CAST(SALDO_CARTERA_VIGENTE AS BIGINT)) AS vigente "
            f"FROM gold.distribucion_cartera "
            f"WHERE ID_ENTIDAD = '{entity_id}' AND {_fecha_filter(fecha_corte)} "
            f"AND SEGMENTO IS NOT NULL GROUP BY SEGMENTO ORDER BY total DESC"
        )
        if r_ent.get("error") or not r_ent.get("data"):
            return f"Sin datos de cartera para entidad {entity_id} en {fecha_corte}."

        r_mkt = delfos_tools.execute_sql(
            f"SELECT SEGMENTO, SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS mkt_total "
            f"FROM gold.distribucion_cartera "
            f"WHERE {_fecha_filter(fecha_corte)} AND SEGMENTO IS NOT NULL GROUP BY SEGMENTO"
        )
        mkt_by_seg = {
            r.get("SEGMENTO", ""): float(r.get("mkt_total") or 0)
            for r in (r_mkt.get("data") or [])
            if not r_mkt.get("error")
        }

        r_cat = delfos_tools.execute_sql(
            f"SELECT TOP 5 DESCRIPCION_CATEGORIA_CARTERA, "
            f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS total "
            f"FROM gold.distribucion_cartera "
            f"WHERE ID_ENTIDAD = '{entity_id}' AND {_fecha_filter(fecha_corte)} "
            f"AND DESCRIPCION_CATEGORIA_CARTERA IS NOT NULL "
            f"GROUP BY DESCRIPCION_CATEGORIA_CARTERA ORDER BY total DESC"
        )

        seg_rows = r_ent["data"]
        entity_total = sum(float(r.get("total") or 0) for r in seg_rows)
        mkt_grand = sum(mkt_by_seg.values())

        out = [f"Cartera por Segmento | Entidad {entity_id} | Fecha {fecha_corte}", "",
               f"Cartera total entidad: {entity_total:,.0f}"]
        if mkt_grand:
            out.append(f"Share total mercado:   {entity_total / mkt_grand * 100:.2f}%")
        out += [
            "",
            f"{'Segmento':<20} | {'Saldo':>18} | {'%Entidad':>9} | {'%Mercado':>9} | {'ICV%':>7}",
            "-" * 72,
        ]
        for r in seg_rows:
            seg = r.get("SEGMENTO") or "Sin segmento"
            total = float(r.get("total") or 0)
            vigente = float(r.get("vigente") or 0)
            icv = vigente / total * 100 if total else 0
            pct_ent = total / entity_total * 100 if entity_total else 0
            mkt_seg = mkt_by_seg.get(seg, 0)
            pct_mkt = total / mkt_seg * 100 if mkt_seg else 0
            out.append(
                f"{seg:<20} | {total:>18,.0f} | {pct_ent:>8.1f}% | {pct_mkt:>8.1f}% | {icv:>6.2f}%"
            )

        out += [
            "",
            "Top 5 Categorias:",
            f"{'Categoria':<45} | {'Saldo':>18} | {'% Entidad':>10}",
            "-" * 78,
        ]
        if not r_cat.get("error") and r_cat.get("data"):
            for r in r_cat["data"]:
                cat = r.get("DESCRIPCION_CATEGORIA_CARTERA") or "Desconocida"
                total = float(r.get("total") or 0)
                out.append(
                    f"{cat:<45} | {total:>18,.0f} | {total / entity_total * 100:>9.1f}%"
                )
        return "\n".join(out)

    @ai_function
    def get_captacion_breakdown(entity_id: str, fecha_corte: str) -> str:
        """Desglosa captacion por categoria (CDT, CDAT, Ahorro, Corrientes, etc.) con tasa costo fondos y share de mercado."""
        try:
            entity_id = _validate_advisor_param(entity_id, "entity_id")
            fecha_corte = _validate_advisor_param(fecha_corte, "fecha_corte", max_length=10)
        except ValueError as e:
            return f"Error de validación: {e}"

        categoria_labels = {
            1: "CDT", 2: "CDAT", 3: "Mercado Monetario",
            4: "Interbancarios", 5: "Repos",
            7: "Cuentas de Ahorro", 8: "Cuentas Corrientes",
        }

        r_ent = delfos_tools.execute_sql(
            f"SELECT CODIGO_CATEGORIA, AVG(TASA) AS tasa_prom, SUM(MONTO) AS monto_total "
            f"FROM gold.tasas_interes_captacion "
            f"WHERE ID_ENTIDAD = '{entity_id}' AND {_fecha_filter(fecha_corte)} "
            f"AND CODIGO_CATEGORIA IS NOT NULL GROUP BY CODIGO_CATEGORIA ORDER BY monto_total DESC"
        )
        if r_ent.get("error") or not r_ent.get("data"):
            return f"Sin datos de captacion para entidad {entity_id} en {fecha_corte}."

        r_mkt = delfos_tools.execute_sql(
            f"SELECT CODIGO_CATEGORIA, SUM(MONTO) AS mkt_monto "
            f"FROM gold.tasas_interes_captacion "
            f"WHERE {_fecha_filter(fecha_corte)} AND CODIGO_CATEGORIA IS NOT NULL "
            f"GROUP BY CODIGO_CATEGORIA"
        )
        mkt_by_cat = {
            int(r.get("CODIGO_CATEGORIA") or 0): float(r.get("mkt_monto") or 0)
            for r in (r_mkt.get("data") or [])
            if not r_mkt.get("error")
        }

        rows = r_ent["data"]
        entity_total = sum(float(r.get("monto_total") or 0) for r in rows)
        mkt_grand = sum(mkt_by_cat.values())
        weighted_rate = (
            sum(float(r.get("tasa_prom") or 0) * float(r.get("monto_total") or 0) for r in rows)
            / entity_total
            if entity_total
            else 0
        )

        out = [f"Captacion por Categoria | Entidad {entity_id} | Fecha {fecha_corte}", "",
               f"Captacion total:          {entity_total:,.0f}",
               f"Tasa costo fondos (pond.): {weighted_rate:.2f}%"]
        if mkt_grand:
            out.append(f"Share total mercado:       {entity_total / mkt_grand * 100:.2f}%")
        out += [
            "",
            f"{'Categoria':<22} | {'Monto':>18} | {'%Entidad':>9} | {'%Mercado':>9} | {'Tasa%':>7}",
            "-" * 76,
        ]
        for r in rows:
            cod = int(r.get("CODIGO_CATEGORIA") or 0)
            label = categoria_labels.get(cod, f"Cat {cod}")
            monto = float(r.get("monto_total") or 0)
            tasa = float(r.get("tasa_prom") or 0)
            pct_ent = monto / entity_total * 100 if entity_total else 0
            mkt_cat = mkt_by_cat.get(cod, 0)
            pct_mkt = monto / mkt_cat * 100 if mkt_cat else 0
            out.append(
                f"{label:<22} | {monto:>18,.0f} | {pct_ent:>8.1f}% | {pct_mkt:>8.1f}% | {tasa:>6.2f}%"
            )
        return "\n".join(out)

    @ai_function
    def correlate_metrics(
        entity_id: str, metric_x: str, metric_y: str, n_periods: int = 12
    ) -> str:
        """Correlacion estadistica (Pearson r, R², outliers) entre dos metricas de una entidad. Metricas: "cartera" | "tasa_credito" | "tasa_captacion"."""
        try:
            entity_id = _validate_advisor_param(entity_id, "entity_id")
        except ValueError as e:
            return f"Error de validación: {e}"

        valid = ("cartera", "tasa_credito", "tasa_captacion")
        if metric_x not in valid or metric_y not in valid:
            return f"Metricas validas: {list(valid)}"
        if metric_x == metric_y:
            return "Error: metric_x y metric_y deben ser diferentes."
        n_periods = max(3, min(24, n_periods))

        sql_map = {
            "cartera": (
                f"SELECT TOP {n_periods} year, month, "
                f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS valor "
                f"FROM gold.distribucion_cartera WHERE ID_ENTIDAD = '{entity_id}' "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
            "tasa_credito": (
                f"SELECT TOP {n_periods} year, month, AVG(TASA_EFECTIVA_PROMEDIO) AS valor "
                f"FROM gold.tasas_interes_credito WHERE ID_ENTIDAD = '{entity_id}' "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
            "tasa_captacion": (
                f"SELECT TOP {n_periods} year, month, AVG(TASA) AS valor "
                f"FROM gold.tasas_interes_captacion WHERE ID_ENTIDAD = '{entity_id}' "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
        }

        def _fetch(metric: str) -> dict[tuple[int, int], float]:
            res = delfos_tools.execute_sql(sql_map[metric])
            if res.get("error"):
                raise RuntimeError(f"Error SQL {metric}: {res['error']}")
            return {
                (int(r["year"]), int(r["month"])): float(r["valor"])
                for r in res.get("data", [])
                if r.get("valor") is not None
            }

        try:
            sx = _fetch(metric_x)
            sy = _fetch(metric_y)
        except RuntimeError as e:
            return str(e)

        shared = sorted(sx.keys() & sy.keys())
        if len(shared) < 3:
            return (
                f"Periodos alineados insuficientes ({len(shared)}). "
                "Se necesitan al menos 3."
            )

        pts = [
            {"x_value": sx[k], "y_value": sy[k], "label": f"{k[0]}-{k[1]:02d}"}
            for k in shared
        ]
        stats = compute_relationship_stats(pts)

        if "warning" in stats and "r" not in stats:
            return f"No se pudo calcular correlacion: {stats['warning']}"

        out = [
            f"Correlacion {metric_x} vs {metric_y} | Entidad {entity_id} | {len(shared)} periodos",
            "",
            f"Pearson r:               {stats['r']:+.4f}",
            f"R² (varianza explicada): {stats['r2']:.4f}  ({stats['r2'] * 100:.1f}%)",
            f"Direccion:               {stats['direction']}",
            f"Intensidad:              {stats['strength']}",
            f"Pendiente:               {stats['slope']:+.6f}",
            "",
            f"Interpretacion: {stats['interpretation']}",
        ]
        if stats.get("warning"):
            out.append(f"Aviso: {stats['warning']}")
        outliers = stats.get("outliers", [])
        if outliers:
            out += ["", f"Outliers ({len(outliers)}):"]
            for o in outliers:
                out.append(
                    f"  {o['label']}: x={o['x']:,.2f}, y={o['y']:,.2f}  "
                    f"(desv: {o['deviation']:+.2f} sigma)"
                )
        return "\n".join(out)

    @ai_function
    def get_credit_quality_breakdown(entity_id: str, fecha_corte: str) -> str:
        """ICV (vigente/total) por segmento y categoria vs mercado, con cartera deteriorada y clasificacion SFC."""
        try:
            entity_id = _validate_advisor_param(entity_id, "entity_id")
            fecha_corte = _validate_advisor_param(fecha_corte, "fecha_corte", max_length=10)
        except ValueError as e:
            return f"Error de validación: {e}"

        def _icv_rating(icv: float) -> str:
            if icv >= 97:
                return "EXCELENTE"
            if icv >= 95:
                return "BUENA"
            if icv >= 90:
                return "ACEPTABLE"
            if icv >= 85:
                return "ATENCION"
            return "CRITICA"

        seg_sql = (
            "SELECT SEGMENTO, "
            "SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS total, "
            "SUM(CAST(SALDO_CARTERA_VIGENTE AS BIGINT)) AS vigente "
            "FROM gold.distribucion_cartera "
            "WHERE {filter} AND SEGMENTO IS NOT NULL GROUP BY SEGMENTO ORDER BY total DESC"
        )
        r_ent = delfos_tools.execute_sql(
            seg_sql.format(filter=f"ID_ENTIDAD = '{entity_id}' AND {_fecha_filter(fecha_corte)}")
        )
        if r_ent.get("error") or not r_ent.get("data"):
            return f"Sin datos para entidad {entity_id} en {fecha_corte}."
        r_mkt = delfos_tools.execute_sql(
            seg_sql.format(filter=f"{_fecha_filter(fecha_corte)}")
        )
        mkt_icv: dict[str, float] = {}
        for row in (r_mkt.get("data") or []):
            t = float(row.get("total") or 0)
            v = float(row.get("vigente") or 0)
            if t > 0:
                mkt_icv[row.get("SEGMENTO", "")] = v / t * 100

        r_cat = delfos_tools.execute_sql(
            f"SELECT TOP 8 DESCRIPCION_CATEGORIA_CARTERA, "
            f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS total, "
            f"SUM(CAST(SALDO_CARTERA_VIGENTE AS BIGINT)) AS vigente "
            f"FROM gold.distribucion_cartera "
            f"WHERE ID_ENTIDAD = '{entity_id}' AND {_fecha_filter(fecha_corte)} "
            f"AND DESCRIPCION_CATEGORIA_CARTERA IS NOT NULL "
            f"GROUP BY DESCRIPCION_CATEGORIA_CARTERA ORDER BY total DESC"
        )

        seg_rows = r_ent["data"]
        grand_total = sum(float(r.get("total") or 0) for r in seg_rows)
        grand_vigente = sum(float(r.get("vigente") or 0) for r in seg_rows)
        grand_icv = grand_vigente / grand_total * 100 if grand_total else 0

        out = [
            f"Calidad de Cartera | Entidad {entity_id} | Fecha {fecha_corte}", "",
            f"ICV Global: {grand_icv:.2f}%  [{_icv_rating(grand_icv)}]",
            f"Cartera total:       {grand_total:>20,.0f}",
            f"Cartera vigente:     {grand_vigente:>20,.0f}",
            f"Cartera deteriorada: {grand_total - grand_vigente:>20,.0f}  ({100 - grand_icv:.2f}%)",
            "", "Por Segmento:",
            f"{'Segmento':<20} | {'ICV Entidad':>12} | {'ICV Mercado':>12} | "
            f"{'Diferencia':>12} | {'Deteriorada':>18}",
            "-" * 84,
        ]
        for r in seg_rows:
            seg = r.get("SEGMENTO") or "Sin segmento"
            t = float(r.get("total") or 0)
            v = float(r.get("vigente") or 0)
            icv = v / t * 100 if t else 0
            m_icv = mkt_icv.get(seg, 0)
            diff = f"{icv - m_icv:+.2f}pp" if m_icv else "N/A"
            m_str = f"{m_icv:.2f}%" if m_icv else "N/A"
            out.append(
                f"{seg:<20} | {icv:>11.2f}% | {m_str:>12} | {diff:>12} | {t - v:>18,.0f}"
            )

        out += [
            "", "Por Categoria (Top 8):",
            f"{'Categoria':<45} | {'ICV%':>7} | {'Deteriorada':>18}",
            "-" * 76,
        ]
        for r in (r_cat.get("data") or []):
            cat = r.get("DESCRIPCION_CATEGORIA_CARTERA") or "Desconocida"
            t = float(r.get("total") or 0)
            v = float(r.get("vigente") or 0)
            icv = v / t * 100 if t else 0
            out.append(
                f"{cat:<45} | {icv:>6.2f}% | {t - v:>18,.0f}  [{_icv_rating(icv)}]"
            )
        return "\n".join(out)

    @ai_function
    def get_market_evolution(metric: str, n_periods: int = 6) -> str:
        """Evolucion del mercado total para distinguir si un cambio es sistemico o especifico. Metric: "cartera" | "tasa_credito" | "tasa_captacion"."""
        valid = ("cartera", "tasa_credito", "tasa_captacion")
        if metric not in valid:
            return f"metric debe ser uno de: {list(valid)}"
        n_periods = max(3, min(24, n_periods))

        sql_map = {
            "cartera": (
                f"SELECT TOP {n_periods} year, month, COUNT(DISTINCT ID_ENTIDAD) AS n_ent, "
                f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS valor "
                f"FROM gold.distribucion_cartera "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
            "tasa_credito": (
                f"SELECT TOP {n_periods} year, month, COUNT(DISTINCT ID_ENTIDAD) AS n_ent, "
                f"AVG(TASA_EFECTIVA_PROMEDIO) AS valor "
                f"FROM gold.tasas_interes_credito "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
            "tasa_captacion": (
                f"SELECT TOP {n_periods} year, month, COUNT(DISTINCT ID_ENTIDAD) AS n_ent, "
                f"AVG(TASA) AS valor "
                f"FROM gold.tasas_interes_captacion "
                f"GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
        }
        result = delfos_tools.execute_sql(sql_map[metric])
        if result.get("error") or not result.get("data"):
            return f"Sin datos de mercado para {metric}."

        rows = [r for r in reversed(result["data"]) if r.get("valor") is not None]
        if not rows:
            return f"Sin datos de mercado para {metric}."
        vals = [float(r["valor"]) for r in rows]
        labels = [f"{int(r['year'])}-{int(r['month']):02d}" for r in rows]
        n_ents = [int(r.get("n_ent") or 0) for r in rows]
        mom = [
            (vals[i] - vals[i - 1]) / abs(vals[i - 1]) * 100
            if vals[i - 1] != 0 else None
            for i in range(1, len(vals))
        ]

        label_map = {
            "cartera": "Cartera Total Mercado",
            "tasa_credito": "Tasa Credito Promedio",
            "tasa_captacion": "Tasa Captacion Promedio",
        }
        is_cop = metric == "cartera"

        out = [
            f"Evolucion Mercado | {label_map[metric]} | {len(rows)} periodos", "",
            f"{'Periodo':<12} | {'Entidades':>10} | {'Valor':>18} | {'MoM%':>8}",
            "-" * 58,
        ]
        for i, (lbl, val, n_ent) in enumerate(zip(labels, vals, n_ents)):
            mom_str = f"{mom[i - 1]:+.2f}%" if i > 0 and mom[i - 1] is not None else "  base"
            val_str = f"{val:>18,.0f}" if is_cop else f"{val:>17.2f}%"
            out.append(f"{lbl:<12} | {n_ent:>10,} | {val_str} | {mom_str:>8}")

        out.append("")
        if len(vals) >= 2 and vals[0] != 0:
            out.append(f"Variacion total: {(vals[-1] - vals[0]) / abs(vals[0]) * 100:+.2f}%")
        if len(vals) >= 4 and vals[-4] != 0:
            out.append(f"QoQ: {(vals[-1] - vals[-4]) / abs(vals[-4]) * 100:+.2f}%")
        valid_mom = [m for m in mom if m is not None]
        if valid_mom:
            out.append(f"MoM promedio: {sum(valid_mom) / len(valid_mom):+.2f}%")
        return "\n".join(out)

    @ai_function
    def get_latest_available_date() -> str:
        """Retorna la fecha mas reciente disponible por fuente. Cada fuente puede tener fechas distintas — usa la fecha_corte correcta segun la herramienta."""
        cached = _cache_get("latest_date")
        if cached is not None:
            return cached
        tables = {
            "cartera": (
                "SELECT TOP 1 year, month "
                "FROM gold.distribucion_cartera "
                "GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
            "tasas_credito": (
                "SELECT TOP 1 year, month "
                "FROM gold.tasas_interes_credito "
                "GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
            "tasas_captacion": (
                "SELECT TOP 1 year, month "
                "FROM gold.tasas_interes_captacion "
                "GROUP BY year, month ORDER BY year DESC, month DESC"
            ),
        }
        tool_map = {
            "cartera": "get_portfolio_breakdown, get_group_consolidated, get_credit_quality_breakdown, peer_benchmark(cartera), detect_anomalies",
            "tasas_credito": "pricing_analysis, peer_benchmark(tasa_credito), trend_analysis(tasa_credito)",
            "tasas_captacion": "get_captacion_breakdown, peer_benchmark(tasa_captacion), trend_analysis(tasa_captacion)",
        }
        out = ["Ultimo periodo disponible por fuente:", ""]
        for label, sql in tables.items():
            result = delfos_tools.execute_sql(sql)
            if result.get("error") or not result.get("data"):
                out.append(f"  {label}: sin datos")
                continue
            row = result["data"][0]
            y, m = int(row["year"]), int(row["month"])
            out.append(f"  {label}: {m:02d}/{y}  (fecha_corte: {m}{y})")
            out.append(f"    Usar para: {tool_map[label]}")
        out.append("")
        out.append("IMPORTANTE: Cada fuente tiene su propia fecha. Usa la fecha_corte de la fuente que corresponda a la herramienta que vas a llamar.")
        return _cache_set("latest_date", "\n".join(out))

    @ai_function
    def get_group_consolidated(entity_ids: str, fecha_corte: str) -> str:
        """Consolida cartera, ICV y market share de multiples entidades. entity_ids separados por coma (ej. "11,12,123")."""
        try:
            fecha_corte = _validate_advisor_param(fecha_corte, "fecha_corte", max_length=10)
        except ValueError as e:
            return f"Error de validación: {e}"

        ids = [eid.strip() for eid in entity_ids.split(",") if eid.strip()]
        if not ids:
            return "Error: entity_ids vacio. Formato: '11,12,123'."
        for eid in ids:
            try:
                _validate_advisor_param(eid, "entity_id")
            except ValueError as e:
                return f"Error de validación: {e}"

        id_list = ", ".join(f"'{eid}'" for eid in ids)
        fecha_sql = _fecha_filter(fecha_corte)

        r_group = delfos_tools.execute_sql(
            f"SELECT ID_ENTIDAD, MAX(NOMBRE_ENTIDAD) AS nombre, "
            f"SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS total, "
            f"SUM(CAST(SALDO_CARTERA_VIGENTE AS BIGINT)) AS vigente "
            f"FROM gold.distribucion_cartera "
            f"WHERE ID_ENTIDAD IN ({id_list}) AND {fecha_sql} "
            f"GROUP BY ID_ENTIDAD ORDER BY total DESC"
        )
        if r_group.get("error") or not r_group.get("data"):
            return f"Sin datos de cartera para entidades {entity_ids} en {fecha_corte}."

        r_mkt = delfos_tools.execute_sql(
            f"SELECT SUM(CAST(SALDO_CARTERA_A_FECHA_CORTE AS BIGINT)) AS mkt_total "
            f"FROM gold.distribucion_cartera WHERE {fecha_sql}"
        )
        mkt_total = 0.0
        if not r_mkt.get("error") and r_mkt.get("data"):
            mkt_total = float(r_mkt["data"][0].get("mkt_total") or 0)

        rows = r_group["data"]
        group_total = sum(float(r.get("total") or 0) for r in rows)
        group_vigente = sum(float(r.get("vigente") or 0) for r in rows)
        group_icv = group_vigente / group_total * 100 if group_total else 0
        group_share = group_total / mkt_total * 100 if mkt_total else 0

        out = [
            f"Consolidado {len(ids)} entidades | Periodo {fecha_corte}",
            "",
            f"Cartera total grupo: {group_total:,.0f}",
            f"ICV grupo:           {group_icv:.2f}%",
            f"Market share grupo:  {group_share:.1f}%",
            f"Mercado total:       {mkt_total:,.0f}",
            "",
            f"{'Banco':<30} | {'Cartera':>18} | {'ICV%':>7} | {'Share Mkt':>10} | {'Share Grupo':>12}",
            "-" * 85,
        ]
        for r in rows:
            nombre = r.get("nombre") or str(r["ID_ENTIDAD"])
            total = float(r.get("total") or 0)
            vigente = float(r.get("vigente") or 0)
            icv = vigente / total * 100 if total else 0
            share_mkt = total / mkt_total * 100 if mkt_total else 0
            share_grp = total / group_total * 100 if group_total else 0
            out.append(
                f"{nombre:<30} | {total:>18,.0f} | {icv:>6.2f}% | {share_mkt:>9.1f}% | {share_grp:>11.1f}%"
            )

        return "\n".join(out)

    @ai_function
    def lookup_entity(search_term: str) -> str:
        """Busca entidades por nombre parcial. Retorna ID, nombre y tipo. Usar antes de otras tools para resolver nombres a IDs."""
        try:
            search_term = _validate_advisor_param(search_term, "search_term", max_length=50)
        except ValueError as e:
            return f"Error de validación: {e}"
        cache_key = f"lookup:{search_term.lower()}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        result = delfos_tools.execute_sql(
            f"SELECT TOP 10 ID_ENTIDAD, NOMBRE_ENTIDAD, NOMBRE_TIPO_ENTIDAD "
            f"FROM gold.banco WHERE NOMBRE_ENTIDAD LIKE '%{search_term}%' "
            f"ORDER BY NOMBRE_ENTIDAD"
        )
        if result.get("error"):
            return f"Error SQL: {result['error']}"
        rows = result.get("data", [])
        if not rows:
            return f"No se encontraron entidades con '{search_term}'."
        out = [f"Entidades encontradas ({len(rows)}):"]
        for r in rows:
            out.append(
                f"  ID: {r['ID_ENTIDAD']} | {r['NOMBRE_ENTIDAD']} | {r['NOMBRE_TIPO_ENTIDAD']}"
            )
        return _cache_set(cache_key, "\n".join(out))

    return [
        lookup_entity,
        query_warehouse,
        get_available_tables,
        get_table_columns,
        get_latest_available_date,
        get_entity_profile,
        trend_analysis,
        peer_benchmark,
        detect_anomalies,
        pricing_analysis,
        get_portfolio_breakdown,
        get_captacion_breakdown,
        correlate_metrics,
        get_credit_quality_breakdown,
        get_market_evolution,
        get_group_consolidated,
    ]
