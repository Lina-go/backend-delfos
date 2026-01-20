"""Plotly chart builders that return styled HTML/PNG bytes."""

import logging
import re
from typing import Any
from collections import defaultdict

import plotly.graph_objects as go

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

ChartArtifacts = dict[str, Any]
config_params = {
    "displayModeBar": True,
    "modeBarButtonsToRemove": [],
    "staticPlot": False,
}


def _apply_styling(fig: go.Figure, title: str) -> None:
    """Apply shared styling similar to plotly_blob_demo."""
    fig.update_layout(
        title=f"<b>{title}</b>",
        title_font={"color": "black", "size": 14, "family": "Inter, Arial, sans-serif"},
        template="plotly_white",
        hovermode="x unified",
        font={"family": "Inter, Arial, sans-serif", "size": 12},
        margin={"l": 60, "r": 140, "t": 90, "b": 50},
        showlegend=True,
        legend={
            "orientation": "v",
            "yanchor": "top",
            "y": 1,
            "xanchor": "left",
            "x": 1.02,
        },
    )
    fig.update_xaxes(
        showgrid=True, gridcolor="rgba(0,0,0,0.05)", showspikes=True, spikemode="across"
    )
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.05)", zeroline=False)


def _is_date_like(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    value = value.strip()
    return bool(re.match(r"^\d{4}-\d{2}(-\d{2})?$", value))


def _infer_axis_labels(
    title: str,
    data_points: list[dict[str, Any]],
    chart_type: str,
) -> tuple[str | None, str | None, str | None]:
    if chart_type == "pie":
        return None, None, None

    x_label = "Categoria"
    sample_x = next((d.get("x_value") for d in data_points if d.get("x_value") is not None), None)
    if _is_date_like(sample_x):
        x_label = "Fecha"

    lower_title = title.lower()
    y_label = "Valor"
    tickformat = ".3s"

    percent_keywords = ["porcentaje", "participacion", "composicion", "ratio", "share"]
    rate_keywords = ["tasa", "interes"]
    count_keywords = ["cantidad", "conteo", "numero", "total"]
    amount_keywords = ["saldo", "monto", "cartera", "valor", "volumen", "ingreso"]

    values = []
    for d in data_points:
        try:
            values.append(float(d.get("y_value", 0)))
        except (TypeError, ValueError):
            continue

    if values and min(values) >= 0 and max(values) <= 1:
        y_label = "Porcentaje (%)"
        tickformat = ".0%"
    elif any(k in lower_title for k in percent_keywords):
        y_label = "Porcentaje (%)"
        tickformat = ".1%"
    elif any(k in lower_title for k in rate_keywords):
        y_label = "Tasa (%)"
        tickformat = ".2%"
    elif any(k in lower_title for k in count_keywords):
        y_label = "Cantidad"
        tickformat = ".3s"
    elif any(k in lower_title for k in amount_keywords):
        y_label = "Monto"
        tickformat = ".3s"

    return x_label, y_label, tickformat


def _render_outputs(fig: go.Figure) -> tuple[bytes, bytes]:
    """Render figure to HTML and PNG bytes.

    This function is intentionally defensive around PNG generation so that
    failures in the underlying browser / Kaleido stack do not break the
    overall graph generation pipeline.
    """
    html_bytes = fig.to_html(
        full_html=True,
        include_plotlyjs="cdn",
        config=config_params,
    ).encode("utf-8")

    png_bytes: bytes
    try:
        png_bytes = fig.to_image(format="png", width=1100, height=650, scale=2)
    except Exception as exc:  # noqa: BLE001
        # If PNG generation fails (e.g. Kaleido / browser issues), keep HTML output
        # and log a warning so the pipeline can continue gracefully.
        logger.warning("PNG generation failed: %s", exc)
        png_bytes = b""

    return html_bytes, png_bytes


def generate_pie_chart(
    data_points: list[dict[str, Any]],
    title: str,
    colors: list[str] | None = None,
) -> ChartArtifacts:
    """Generate a styled pie chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette

    # Limit pie slices to 7 by grouping the tail into "Otros".
    pie_rows = []
    for d in data_points:
        y_value = d.get("y_value", 0)
        try:
            numeric_value = float(y_value)
        except (TypeError, ValueError):
            numeric_value = 0.0
        pie_rows.append(
            {
                "label": d.get("x_value", ""),
                "value": numeric_value,
            }
        )

    pie_rows.sort(key=lambda row: row["value"], reverse=True)
    if len(pie_rows) > 7:
        top_rows = pie_rows[:6]
        other_total = sum(row["value"] for row in pie_rows[6:])
        if other_total > 0:
            top_rows.append({"label": "Otros", "value": other_total})
        pie_rows = top_rows

    labels = [row["label"] for row in pie_rows]
    values = [row["value"] for row in pie_rows]

    fig = go.Figure(
        data=[go.Pie(labels=labels, values=values, marker_colors=colors[: len(labels)])]
    )
    _apply_styling(fig, title)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}


def generate_bar_chart(
    data_points: list[dict[str, Any]],
    title: str,
    colors: list[str] | None = None,
) -> ChartArtifacts:
    """Generate a styled bar chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette

    x_values = [d.get("x_value", "") for d in data_points]
    y_values = [d.get("y_value", 0) for d in data_points]

    fig = go.Figure(data=[go.Bar(x=x_values, y=y_values, marker_color=colors[0])])
    x_label, y_label, tickformat = _infer_axis_labels(title, data_points, "bar")
    _apply_styling(fig, title)
    fig.update_xaxes(title=x_label or None)
    fig.update_yaxes(title=y_label or None, tickformat=tickformat)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}


def generate_line_chart(
    data_points: list[dict[str, Any]],
    title: str,
    colors: list[str] | None = None,
) -> ChartArtifacts:
    """Generate a styled line chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette
 
    series_data: dict[str, dict[str, list]] = defaultdict(lambda: {"x": [], "y": []})
    for d in data_points:
        category = d.get("category") or d.get("series") or "default"
        series_data[category]["x"].append(d.get("x_value", ""))
        series_data[category]["y"].append(d.get("y_value", 0))
 
    fig = go.Figure()
    series_totals = {
        category: sum(
            float(val) if isinstance(val, (int, float)) else 0 for val in values.get("y", [])
        )
        for category, values in series_data.items()
    }
    ordered_series = sorted(series_data.items(), key=lambda item: series_totals.get(item[0], 0), reverse=True)

    for idx, (category, values) in enumerate(ordered_series):
        color = colors[idx % len(colors)]
        fig.add_trace(
            go.Scatter(
                x=values["x"],
                y=values["y"],
                mode="lines+markers",
                name=category,
                line_color=color,
            )
        )
    x_label, y_label, tickformat = _infer_axis_labels(title, data_points, "line")
    _apply_styling(fig, title)
    fig.update_xaxes(title=x_label or None)
    fig.update_yaxes(title=y_label or None, tickformat=tickformat)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}

def generate_stacked_bar_chart(
    data_points: list[dict[str, Any]],
    title: str,
    colors: list[str] | None = None,
) -> ChartArtifacts:
    """Generate a styled stacked bar chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette

    categories: dict[str, list[dict[str, Any]]] = {}
    for d in data_points:
        cat = d.get("category") or d.get("series") or "default"
        categories.setdefault(cat, []).append(d)

    fig = go.Figure()
    cat_totals = {}
    for cat, points in categories.items():
        total = 0.0
        for p in points:
            try:
                total += float(p.get("y_value", 0))
            except (TypeError, ValueError):
                continue
        cat_totals[cat] = total

    ordered_categories = sorted(categories.items(), key=lambda item: cat_totals.get(item[0], 0), reverse=True)

    for idx, (cat, points) in enumerate(ordered_categories):
        x_values = [p.get("x_value", "") for p in points]
        y_values = [p.get("y_value", 0) for p in points]
        fig.add_trace(
            go.Bar(
                name=cat,
                x=x_values,
                y=y_values,
                marker_color=colors[idx % len(colors)],
            )
        )

    fig.update_layout(barmode="stack")
    x_label, y_label, tickformat = _infer_axis_labels(title, data_points, "stackedbar")
    _apply_styling(fig, title)
    fig.update_xaxes(title=x_label or None)
    fig.update_yaxes(title=y_label or None, tickformat=tickformat)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}



