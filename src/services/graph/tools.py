"""AI functions for graph generation."""

import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Any
from src.config.constants import ChartType
from src.config.settings import get_settings
import base64
from io import BytesIO

def generate_pie_chart(
    data_points: List[Dict[str, Any]],
    colors: List[str] = None,
) -> str:
    """
    Generate a pie chart image.
    
    Args:
        data_points: List of dicts with x_value, y_value, category
        colors: Color palette (defaults to settings chart_color_palette)
        
    Returns:
        Base64 encoded image string
    """
    if colors is None:
        colors = get_settings().chart_color_palette
    
    labels = [d.get("x_value", "") for d in data_points]
    values = [d.get("y_value", 0) for d in data_points]
    
    fig = go.Figure(data=[go.Pie(labels=labels, values=values, marker_colors=colors[:len(labels)])])
    fig.update_layout(title="Pie Chart")
    
    # Convert to base64
    img_bytes = fig.to_image(format="png")
    return base64.b64encode(img_bytes).decode()


def generate_bar_chart(
    data_points: List[Dict[str, Any]],
    colors: List[str] = None,
) -> str:
    """Generate a bar chart image."""
    if colors is None:
        colors = get_settings().chart_color_palette
    
    x_values = [d.get("x_value", "") for d in data_points]
    y_values = [d.get("y_value", 0) for d in data_points]
    
    fig = go.Figure(data=[go.Bar(x=x_values, y=y_values, marker_color=colors[0])])
    fig.update_layout(title="Bar Chart")
    
    img_bytes = fig.to_image(format="png")
    return base64.b64encode(img_bytes).decode()


def generate_line_chart(
    data_points: List[Dict[str, Any]],
    colors: List[str] = None,
) -> str:
    """Generate a line chart image."""
    if colors is None:
        colors = get_settings().chart_color_palette
    
    x_values = [d.get("x_value", "") for d in data_points]
    y_values = [d.get("y_value", 0) for d in data_points]
    
    fig = go.Figure(data=[go.Scatter(x=x_values, y=y_values, mode='lines+markers', line_color=colors[0])])
    fig.update_layout(title="Line Chart")
    
    img_bytes = fig.to_image(format="png")
    return base64.b64encode(img_bytes).decode()


def generate_stacked_bar_chart(
    data_points: List[Dict[str, Any]],
    colors: List[str] = None,
) -> str:
    """Generate a stacked bar chart image."""
    if colors is None:
        colors = get_settings().chart_color_palette
    
    # Group by category
    categories = {}
    for d in data_points:
        cat = d.get("category", "default")
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(d)
    
    fig = go.Figure()
    for idx, (cat, points) in enumerate(categories.items()):
        x_values = [p.get("x_value", "") for p in points]
        y_values = [p.get("y_value", 0) for p in points]
        fig.add_trace(go.Bar(
            name=cat,
            x=x_values,
            y=y_values,
            marker_color=colors[idx % len(colors)]
        ))
    
    fig.update_layout(barmode='stack', title="Stacked Bar Chart")
    
    img_bytes = fig.to_image(format="png")
    return base64.b64encode(img_bytes).decode()

