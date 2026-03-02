"""KPI indicator computation for visualizations."""

from __future__ import annotations

import logging
import statistics
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.services.chat_v2.models import IndicatorSpec

logger = logging.getLogger(__name__)

# Cross-series calc types that compare ALL series (not a single one).
_CROSS_SERIES_CALCS = {"max_change", "rank_change", "share_of_growth", "growth_vs_market"}


def _fmt_co(value: float, decimals: int = 2) -> str:
    """Format number in Colombian locale (dot thousands, comma decimals)."""
    raw = f"{value:,.{decimals}f}"
    return raw.replace(",", "X").replace(".", ",").replace("X", ".")


def _format_value(value: float, unit: str) -> str:
    sign = "+" if value > 0 else ""
    if unit == "pp":
        return f"{sign}{_fmt_co(value)} pp"
    if unit == "bps":
        return f"{sign}{_fmt_co(value * 100, 0)} bps"
    if unit == "%":
        return f"{sign}{_fmt_co(value)} %"
    if unit == "abs":
        return _fmt_co(value)
    return f"{sign}{_fmt_co(value)}"


def _color(value: float) -> str:
    if abs(value) < 1e-9:
        return "neutral"
    return "positive" if value > 0 else "negative"


def compute_series_stats(
    data_points: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compute first/last/prev boundary values per series from data_points."""
    series_map: dict[str, list[dict[str, Any]]] = {}
    for dp in data_points:
        s = dp.get("series", dp.get("category", "Valor"))
        series_map.setdefault(s, []).append(dp)

    stats: dict[str, dict[str, Any]] = {}
    for series_name, points in series_map.items():
        sorted_pts = sorted(points, key=lambda p: str(p.get("x_value", "")))
        if len(sorted_pts) < 2:
            continue

        first, last = sorted_pts[0], sorted_pts[-1]
        prev = sorted_pts[-2]

        stats[series_name] = {
            "first_x": first["x_value"],
            "first_y": float(first["y_value"]),
            "last_x": last["x_value"],
            "last_y": float(last["y_value"]),
            "prev_x": prev["x_value"],
            "prev_y": float(prev["y_value"]),
            "count": len(sorted_pts),
        }

    return stats


def compute_full_series_stats(
    data_points: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compute full time-series statistics per series, including all y-values.

    Returns a superset of ``compute_series_stats`` output, adding:
      - all_y: list[float] sorted by x_value
      - all_x: list[str] sorted
      - std_dev: float (sample standard deviation)
      - mean_y: float
    """
    base = compute_series_stats(data_points)

    # Re-group and sort to extract all_y/all_x (base only stores boundaries).
    series_map: dict[str, list[dict[str, Any]]] = {}
    for dp in data_points:
        s = dp.get("series", dp.get("category", "Valor"))
        series_map.setdefault(s, []).append(dp)

    for series_name, s in base.items():
        sorted_pts = sorted(
            series_map[series_name],
            key=lambda p: str(p.get("x_value", "")),
        )
        all_y = [float(p["y_value"]) for p in sorted_pts]
        all_x = [str(p["x_value"]) for p in sorted_pts]
        s["all_y"] = all_y
        s["all_x"] = all_x
        s["std_dev"] = statistics.stdev(all_y) if len(all_y) >= 2 else 0.0
        s["mean_y"] = statistics.mean(all_y)

    return base


def _apply_calc(
    s: dict[str, Any], calc: str, unit: str,
) -> float | None:
    """Apply an arithmetic operation to series boundary stats."""
    first_y, last_y, prev_y = s["first_y"], s["last_y"], s["prev_y"]

    if calc == "period_delta":
        if unit in ("pp", "bps"):
            return last_y - first_y
        return ((last_y / first_y) - 1) * 100 if abs(first_y) > 1e-9 else 0.0

    if calc == "pct_change":
        return ((last_y / first_y) - 1) * 100 if abs(first_y) > 1e-9 else 0.0

    if calc == "prev_delta":
        if unit in ("pp", "bps"):
            return last_y - prev_y
        return ((last_y / prev_y) - 1) * 100 if abs(prev_y) > 1e-9 else 0.0

    if calc == "momentum":
        all_y = s.get("all_y", [])
        if len(all_y) < 4:
            return None
        mid = len(all_y) // 2
        if unit in ("pp", "bps"):
            slope_first = (all_y[mid - 1] - all_y[0]) / max(mid - 1, 1)
            slope_second = (all_y[-1] - all_y[mid]) / max(len(all_y) - mid - 1, 1)
        else:
            # Normalize: percentage growth per period for each half.
            if abs(all_y[0]) > 1e-9:
                slope_first = ((all_y[mid - 1] / all_y[0]) - 1) * 100 / max(mid - 1, 1)
            else:
                slope_first = 0.0
            if abs(all_y[mid]) > 1e-9:
                slope_second = ((all_y[-1] / all_y[mid]) - 1) * 100 / max(len(all_y) - mid - 1, 1)
            else:
                slope_second = 0.0
        return slope_second - slope_first

    logger.warning("[INDICATORS] Unknown calc type: %s", calc)
    return None


def _ordinal_es(n: int) -> str:
    """Return Spanish ordinal: 1 -> '1ro', 2 -> '2do', 3 -> '3ro', etc."""
    if n == 1:
        return "1ro"
    if n == 2:
        return "2do"
    if n == 3:
        return "3ro"
    return f"{n}to"


def _resolve_cross_series(
    series_stats: dict[str, dict[str, Any]],
    spec: IndicatorSpec,
) -> dict[str, Any] | None:
    """Resolve a cross-series indicator by comparing ALL series."""
    if len(series_stats) < 2:
        return None

    if spec.calc == "max_change":
        changes = {name: s["last_y"] - s["first_y"] for name, s in series_stats.items()}
        best = max(changes, key=lambda k: abs(changes[k]))
        val = round(changes[best], 4)
        direction = "mayor aumento" if val > 0 else "mayor caida"
        return {
            "label": spec.label,
            "value": val,
            "formatted": _format_value(val, spec.unit),
            "unit": spec.unit,
            "color": _color(val),
            "detail": f"{best} ({direction})",
            "series": best,
        }

    if spec.calc == "rank_change":
        ranked_first = sorted(series_stats.items(), key=lambda kv: kv[1]["first_y"], reverse=True)
        ranked_last = sorted(series_stats.items(), key=lambda kv: kv[1]["last_y"], reverse=True)
        rank_first = {name: i + 1 for i, (name, _) in enumerate(ranked_first)}
        rank_last = {name: i + 1 for i, (name, _) in enumerate(ranked_last)}

        rank_deltas = {name: rank_first[name] - rank_last[name] for name in series_stats}
        best = max(rank_deltas, key=lambda k: abs(rank_deltas[k]))
        movement = rank_deltas[best]

        if movement == 0:
            return None

        direction = "subio" if movement > 0 else "bajo"
        positions = abs(movement)
        pos_word = "posicion" if positions == 1 else "posiciones"
        return {
            "label": spec.label,
            "value": float(movement),
            "formatted": f"{direction} {positions} {pos_word}",
            "unit": "pos",
            "color": "positive" if movement > 0 else "negative",
            "detail": f"{best}: {_ordinal_es(rank_first[best])} -> {_ordinal_es(rank_last[best])}",
            "series": best,
        }

    if spec.calc == "share_of_growth":
        # SoG_i = (delta_i / total_delta) * 100
        deltas = {name: s["last_y"] - s["first_y"] for name, s in series_stats.items()}
        total_delta = sum(deltas.values())
        if abs(total_delta) < 1e-9:
            return None
        sog = {name: (d / total_delta) * 100 for name, d in deltas.items()}
        best = max(sog, key=lambda k: sog[k])
        val = round(sog[best], 4)
        return {
            "label": spec.label,
            "value": val,
            "formatted": f"{_fmt_co(val)} %",
            "unit": "%",
            "color": _color(val),
            "detail": f"{best} capturo {_fmt_co(val)}% del crecimiento",
            "series": best,
        }

    if spec.calc == "growth_vs_market":
        # Spread_i = Crec_banco_i - Crec_mercado
        sum_first = sum(s["first_y"] for s in series_stats.values())
        sum_last = sum(s["last_y"] for s in series_stats.values())
        if abs(sum_first) < 1e-9:
            return None
        market_growth = (sum_last / sum_first - 1) * 100
        spreads: dict[str, float] = {}
        for name, s in series_stats.items():
            if abs(s["first_y"]) < 1e-9:
                continue
            entity_growth = (s["last_y"] / s["first_y"] - 1) * 100
            spreads[name] = entity_growth - market_growth
        if not spreads:
            return None
        positive = {k: v for k, v in spreads.items() if v > 0}
        if positive:
            best = max(positive, key=positive.get)  # type: ignore[arg-type]
        else:
            best = max(spreads, key=lambda k: abs(spreads[k]))
        val = round(spreads[best], 4)
        entity_g = round(
            (series_stats[best]["last_y"] / series_stats[best]["first_y"] - 1) * 100, 2,
        )
        return {
            "label": spec.label,
            "value": val,
            "formatted": _format_value(val, "pp"),
            "unit": "pp",
            "color": _color(val),
            "detail": f"{best}: {_fmt_co(entity_g)}% vs mercado {_fmt_co(round(market_growth, 2))}%",
            "series": best,
        }

    return None


def resolve_indicators(
    series_stats: dict[str, dict[str, Any]],
    indicator_specs: list[IndicatorSpec],
) -> list[dict[str, Any]]:
    """Resolve indicator specs into formatted indicator dicts.

    Supports both per-series calcs (period_delta, momentum, …) and
    cross-series calcs (max_change, rank_change, share_of_growth,
    growth_vs_market) that compare ALL series and pick the most relevant one.
    """
    if not indicator_specs or not series_stats:
        return []

    results: list[dict[str, Any]] = []
    default_series = next(iter(series_stats))

    for spec in indicator_specs:
        # --- Cross-series indicators ---
        if spec.calc in _CROSS_SERIES_CALCS:
            indicator = _resolve_cross_series(series_stats, spec)
            if indicator:
                results.append(indicator)
            continue

        # --- Per-series indicators ---
        series_key = spec.series if spec.series and spec.series in series_stats else default_series
        s = series_stats.get(series_key)
        if not s:
            continue

        value = _apply_calc(s, spec.calc, spec.unit)
        if value is None:
            continue

        value = round(value, 4)

        if spec.calc == "prev_delta":
            detail = f"{s['prev_x']} -> {s['last_x']}"
        elif spec.calc == "momentum":
            detail = "Aceleracion: pendiente reciente vs inicial"
        else:
            detail = f"{s['first_x']} -> {s['last_x']}"

        results.append({
            "label": spec.label,
            "value": value,
            "formatted": _format_value(value, spec.unit),
            "unit": spec.unit,
            "color": _color(value),
            "detail": detail,
            "series": series_key if spec.series else spec.series,
        })

    # Deduplicate: drop indicators that share the same rounded value + series.
    seen_values: set[tuple[float, str | None]] = set()
    deduped: list[dict[str, Any]] = []
    for ind in results:
        key = (ind["value"], ind.get("series"))
        if key not in seen_values:
            seen_values.add(key)
            deduped.append(ind)
        else:
            logger.info(
                "[INDICATORS] Dropped redundant indicator '%s' (value=%s, series=%s)",
                ind["label"], ind["value"], ind.get("series"),
            )

    return deduped



def infer_units(llm_specs: list[IndicatorSpec], is_tasa: bool) -> str:
    """Infer delta unit from LLM-provided specs.

    The LLM has question context so its unit choice takes priority.
    Falls back to is_tasa heuristic only when the LLM provided no delta specs.
    """
    for spec in llm_specs:
        if spec.calc in ("period_delta", "prev_delta") and spec.unit in ("pp", "bps", "%"):
            return spec.unit
    return "pp" if is_tasa else "%"


def ensure_minimum_indicators(
    llm_specs: list[IndicatorSpec],
    series_stats: dict[str, dict[str, Any]],
    sub_type: str,
    is_tasa: bool = False,
) -> list[IndicatorSpec]:
    """Guarantee minimum indicator specs, supplementing whatever the LLM provided.

    The LLM determines units via the prompt; this function only fills gaps
    for missing calcs using the same units the LLM chose.
    """
    from src.services.chat_v2.models import IndicatorSpec as Spec

    if sub_type not in ("tendencia_simple", "tendencia_comparada"):
        return llm_specs

    delta_unit = infer_units(llm_specs, is_tasa)
    existing = {(s.calc, s.series) for s in llm_specs}
    extra: list[Spec] = []

    if sub_type == "tendencia_simple":
        default_series = next(iter(series_stats), None)
        has_enough_points = (
            default_series
            and series_stats[default_series].get("count", 0) > 2
        )
        guaranteed = [
            ("period_delta", "Cambio en el periodo", delta_unit),
            ("prev_delta", "Cambio vs mes anterior", delta_unit),
        ]
        for calc, label, unit in guaranteed:
            if calc == "prev_delta" and not has_enough_points:
                continue
            if (calc, None) not in existing:
                extra.append(Spec(label=label, calc=calc, unit=unit, series=None))

    elif sub_type == "tendencia_comparada":
        # Skip per-series period_delta when cross-series calcs already present
        # (max_change already shows the biggest delta across all series).
        existing_calcs = {s.calc for s in llm_specs}
        has_cross_series = bool(existing_calcs & _CROSS_SERIES_CALCS)
        if not has_cross_series:
            for series_name in series_stats:
                if ("period_delta", series_name) not in existing:
                    extra.append(Spec(
                        label=f"Cambio {series_name}",
                        calc="period_delta",
                        unit=delta_unit,
                        series=series_name,
                    ))

    if extra:
        logger.info("[INDICATORS] Added %d guaranteed indicators for %s", len(extra), sub_type)

    # Deduplicate by (calc, series), LLM specs take priority
    seen: set[tuple[str, str | None]] = set()
    deduped: list[Spec] = []
    for spec in [*llm_specs, *extra]:
        key = (spec.calc, spec.series)
        if key not in seen:
            seen.add(key)
            deduped.append(spec)

    if len(deduped) > 5:
        logger.info("[INDICATORS] Capping from %d to 5 indicators", len(deduped))
        deduped = deduped[:5]

    return deduped
